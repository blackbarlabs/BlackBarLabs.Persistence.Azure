using BlackBarLabs.Persistence.Azure;
using BlackBarLabs.Persistence.Azure.StorageTables;
using EastFive.Azure.StorageTables.Driver;
using EastFive.Collections.Generic;
using EastFive.Extensions;
using EastFive.Linq;
using EastFive.Linq.Async;
using EastFive.Linq.Expressions;
using EastFive.Reflection;
using EastFive.Serialization;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Net.Http;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace EastFive.Persistence.Azure.StorageTables.Driver
{
    public class AzureTableDriverDynamic
    {
        protected const int DefaultNumberOfTimesToRetry = 10;
        protected static readonly TimeSpan DefaultBackoffForRetry = TimeSpan.FromSeconds(4);
        protected readonly ExponentialRetry retryPolicy = new ExponentialRetry(DefaultBackoffForRetry, DefaultNumberOfTimesToRetry);

        public readonly CloudTableClient TableClient;
        public readonly CloudBlobClient BlobClient;
        private const int retryHttpStatus = 200;

        private readonly Exception retryException = new Exception();

        #region Init / Setup / Utility

        public AzureTableDriverDynamic(CloudStorageAccount storageAccount)
        {
            TableClient = storageAccount.CreateCloudTableClient();
            TableClient.DefaultRequestOptions.RetryPolicy = retryPolicy;

            BlobClient = storageAccount.CreateCloudBlobClient();
            BlobClient.DefaultRequestOptions.RetryPolicy = retryPolicy;
        }

        public static AzureTableDriverDynamic FromSettings(string settingKey = EastFive.Azure.Persistence.AppSettings.Storage)
        {
            return EastFive.Web.Configuration.Settings.GetString(settingKey,
                (storageString) => FromStorageString(storageString),
                (why) => throw new Exception(why));
        }

        public static AzureTableDriverDynamic FromStorageString(string storageSetting)
        {
            var cloudStorageAccount = CloudStorageAccount.Parse(storageSetting);
            var azureStorageRepository = new AzureTableDriverDynamic(cloudStorageAccount);
            return azureStorageRepository;
        }

        private CloudTable TableFromEntity<TEntity>()
        {
            var tableName = typeof(TEntity).GetCustomAttribute<TableEntityAttribute, string>(
                attr => attr.TableName,
                () => typeof(TEntity).Name);

            var table = TableClient.GetTableReference(tableName);
            return table;
        }

        #endregion

        #region ITableEntity Management

        private CloudTable GetTable<T>()
        {
            var tableName = typeof(T).Name.ToLower();
            return TableClient.GetTableReference(tableName);
        }
        
        private class TableEntity<EntityType> : ITableEntity
        {
            public EntityType Entity { get; private set; }

            public virtual string RowKey
            {
                get
                {
                    var properties = typeof(EntityType)
                        .GetMembers()
                        .ToArray();
                    var attributesKvp = properties
                        .Where(propInfo => propInfo.ContainsCustomAttribute<StoragePropertyAttribute>())
                        .Select(propInfo => propInfo.PairWithKey(propInfo.GetCustomAttribute<StoragePropertyAttribute>()))
                        .ToArray();
                    var rowKeyProperty = attributesKvp
                        .First<KeyValuePair<StoragePropertyAttribute, MemberInfo>, MemberInfo>(
                            (attr, next) =>
                            {
                                if (attr.Key.IsRowKey)
                                    return attr.Value;
                                return next();
                            },
                            () => throw new Exception("Entity does not contain row key attribute"));

                    var rowKeyValue = rowKeyProperty.GetValue(Entity);
                    if(rowKeyValue.GetType().IsSubClassOfGeneric(typeof(IReferenceable)))
                    {
                        var rowKeyRef = rowKeyValue as IReferenceable;
                        var rowKeyRefString = rowKeyRef.id.AsRowKey();
                        return rowKeyRefString;
                    }
                    var rowKeyString = ((Guid)rowKeyValue).AsRowKey();
                    return rowKeyString;
                }
                set
                {
                    var x = value.GetType();
                }
            }

            public string PartitionKey
            {
                get
                {
                    var partitionModificationProperties = typeof(EntityType)
                        .GetMembers()
                        .Where(propInfo => propInfo.ContainsAttributeInterface<IModifyAzureStorageTablePartitionKey>())
                        .Select(propInfo => propInfo.GetAttributesInterface<IModifyAzureStorageTablePartitionKey>().PairWithKey(propInfo))
                        .Where(propInfoKvp => propInfoKvp.Value.Any());
                    if (!partitionModificationProperties.Any())
                        throw new Exception("Entity does not contain partition key attribute");

                    var partitionModificationProperty = partitionModificationProperties.First();
                    var partitionKeyProperty = partitionModificationProperty.Key;
                    var partitionKeyGenerator = partitionModificationProperty.Value.First();
                    var partitionKeyValue = partitionKeyProperty.GetValue(Entity);

                    var partitionKey = partitionKeyGenerator.GeneratePartitionKey(this.RowKey, partitionKeyValue, partitionKeyProperty);
                    return partitionKey;
                }
                set
                {
                    var x = value.GetType();

                }
            }

            public DateTimeOffset Timestamp { get; set; }

            public virtual string ETag { get; set; }

            private IEnumerable<KeyValuePair<MemberInfo, IPersistInAzureStorageTables>> StorageProperties
            {
                get
                {
                    var type = typeof(EntityType);
                    return StorageProperties(type);
                }
            }
            
            public void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
            {
                this.Entity = CreateEntityInstance<EntityType>(properties);
            }

            public IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
            {
                var valuesToStore = StorageProperties
                    .SelectMany(
                        (propInfoAttribute) =>
                        {
                            var propInfo = propInfoAttribute.Key;
                            var attr = propInfoAttribute.Value;
                            var value = propInfo.GetValue(this.Entity);
                            return attr.ConvertValue(value, propInfo);
                        })
                    .ToDictionary();
                return valuesToStore;
            }

            internal static ITableEntity Create<TEntity>(TEntity entity)
            {
                var creatableEntity = new TableEntity<TEntity>();
                creatableEntity.Entity = entity;
                creatableEntity.ETag = "*";
                return creatableEntity;
            }
        }

        private class DeletableEntity<EntityType> : TableEntity<EntityType>
        {
            private Guid rowKeyValue;

            public override string RowKey
            {
                get => this.rowKeyValue.AsRowKey();
                set => base.RowKey = value;
            }

            public override string ETag
            {
                get
                {
                    return "*";
                }
                set
                {
                }
            }

            internal static ITableEntity Delete(Guid rowKey)
            {
                var deletableEntity = new DeletableEntity<EntityType>();
                deletableEntity.rowKeyValue = rowKey;
                return deletableEntity;
            }
        }

        private static IEnumerable<KeyValuePair<MemberInfo, IPersistInAzureStorageTables>> StorageProperties(Type entityType)
        {
            return entityType
                .GetMembers(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.FlattenHierarchy)
                .Where(propInfo => propInfo.ContainsAttributeInterface<IPersistInAzureStorageTables>())
                .Select(
                    propInfo =>
                    {
                        var attrs = propInfo.GetAttributesInterface<IPersistInAzureStorageTables>();
                        if (attrs.Length > 1)
                        {
                            var propIdentifier = $"{propInfo.DeclaringType.FullName}__{propInfo.Name}";
                            var attributesInConflict = attrs.Select(a => a.GetType().FullName).Join(",");
                            throw new Exception($"{propIdentifier} has multiple IPersistInAzureStorageTables attributes:{attributesInConflict}.");
                        }
                        var attr = attrs.First() as IPersistInAzureStorageTables;
                        return attr.PairWithKey(propInfo);
                    });
        }

        private static TEntity CreateEntityInstance<TEntity>(IDictionary<string, EntityProperty> properties)
        {
            var entity = Activator.CreateInstance<TEntity>();
            var storageProperties = StorageProperties(typeof(TEntity));
            foreach (var propInfoAttribute in storageProperties)
            {
                var propInfo = propInfoAttribute.Key;
                var attr = propInfoAttribute.Value;
                var value = attr.GetMemberValue(propInfo, properties);
                propInfo.SetValue(ref entity, value);
            }
            return entity;

            //var entityPopulated = typeof(TEntity)
            //    .GetMembers()
            //    .Where(propInfo => propInfo.ContainsAttributeInterface<IPersistInAzureStorageTables>())
            //    .Aggregate(entityInitial,
            //        (entity, propInfo) =>
            //        {
            //            var attrs = propInfo.GetAttributesInterface<IPersistInAzureStorageTables>();
            //            var attr = attrs.First();
            //            var key = attr.Name;
            //            if (key.IsNullOrWhiteSpace())
            //                key = propInfo.Name;

            //            var convertedValue = attr.GetMemberValue(propInfo, properties);
            //            propInfo.SetValue(ref entity, convertedValue);
            //            return entity;
            //        });

            //return entityPopulated;
        }

        #endregion

        #region Core
        
        public async Task<TResult> CreateAsync<TEntity, TResult>(TEntity entity,
            Func<Guid, TResult> onSuccess,
            Func<TResult> onAlreadyExists,
            Func<ExtendedErrorInformationCodes, string, TResult> onFailure =
                default(Func<ExtendedErrorInformationCodes, string, TResult>),
           AzureStorageDriver.RetryDelegate onTimeout = default(AzureStorageDriver.RetryDelegate))
        {
            var tableEntity = TableEntity<TEntity>.Create(entity);
            while (true)
            {
                var tableName = typeof(TEntity).GetCustomAttribute<TableEntityAttribute, string>(
                    attr => attr.TableName,
                    () => typeof(TEntity).Name);
                var table = TableClient.GetTableReference(tableName);
                try
                {
                    var insert = TableOperation.Insert(tableEntity);
                    var tableResult = await table.ExecuteAsync(insert);
                    return onSuccess(Guid.Parse(tableEntity.RowKey));
                }
                catch (StorageException ex)
                {
                    if (ex.IsProblemResourceAlreadyExists())
                        return onAlreadyExists();

                    var shouldRetry = await ex.ResolveCreate(table,
                        () => true,
                        onTimeout);
                    if (shouldRetry)
                        continue;

                    throw;
                }
                catch (Exception general_ex)
                {
                    var message = general_ex;
                    throw;
                }

            }
        }
        
        public async Task<TResult> FindByIdAsync<TEntity, TResult>(
                string rowKey, string partitionKey,
            Func<TEntity, TResult> onSuccess,
            Func<TResult> onNotFound,
            Func<ExtendedErrorInformationCodes, string, TResult> onFailure =
                default(Func<ExtendedErrorInformationCodes, string, TResult>),
            AzureStorageDriver.RetryDelegate onTimeout =
                default(AzureStorageDriver.RetryDelegate))
        {
            var operation = TableOperation.Retrieve(partitionKey, rowKey,
                (string partitionKeyEntity, string rowKeyEntity, DateTimeOffset timestamp, IDictionary<string, EntityProperty> properties, string etag) =>
                {
                    var entityPopulated = CreateEntityInstance<TEntity>(properties);
                    return entityPopulated;
                });
            var table = TableFromEntity<TEntity>();
            try
            {
                var result = await table.ExecuteAsync(operation);
                if (404 == result.HttpStatusCode)
                    return onNotFound();
                return onSuccess((TEntity)result.Result);
            }
            catch (StorageException se)
            {
                if (se.IsProblemTableDoesNotExist())
                    return onNotFound();
                if (se.IsProblemTimeout())
                {
                    TResult result = default(TResult);
                    if (default(AzureStorageDriver.RetryDelegate) == onTimeout)
                        onTimeout = AzureStorageDriver.GetRetryDelegate();
                    await onTimeout(se.RequestInformation.HttpStatusCode, se,
                        async () =>
                        {
                            result = await FindByIdAsync(rowKey, partitionKey, onSuccess, onNotFound, onFailure, onTimeout);
                        });
                    return result;
                }
                throw se;
            }

        }
        
        private static MemberInfo ResolveMemberInType(Type entityType, MemberExpression expression)
        {
            var member = expression.Member;
            if (entityType.GetMembers().Contains(member))
            {
                if(member.ContainsCustomAttribute<StoragePropertyAttribute>())
                    return member;
                throw new ArgumentException($"{member.DeclaringType.FullName}..{member.Name} is not storage property/field.");
            }

            if (expression.Expression is MemberExpression)
                return ResolveMemberInType(entityType, expression.Expression as MemberExpression);

            throw new ArgumentException($"{member.DeclaringType.FullName}..{member.Name} is not a property/field of {entityType.FullName}.");
        }

        private static string ExpressionTypeToQueryComparison(ExpressionType comparision)
        {
            if (ExpressionType.Equal == comparision)
                return QueryComparisons.Equal;
            if (ExpressionType.Assign == comparision) // why not
                return QueryComparisons.Equal;
            if (ExpressionType.GreaterThan == comparision)
                return QueryComparisons.GreaterThan;
            if (ExpressionType.GreaterThanOrEqual == comparision)
                return QueryComparisons.GreaterThanOrEqual;
            if (ExpressionType.LessThan == comparision)
                return QueryComparisons.LessThan;
            if (ExpressionType.LessThanOrEqual == comparision)
                return QueryComparisons.LessThanOrEqual;

            throw new ArgumentException($"{comparision} is not a supported query comparison.");
        }

        private static string WhereExpression(ExpressionType comparision, string assignmentName, object assignmentValue)
        {
            var queryComparison = ExpressionTypeToQueryComparison(comparision);

            if (typeof(Guid?).IsInstanceOfType(assignmentValue))
                TableQuery.GenerateFilterConditionForGuid(assignmentName, queryComparison, (assignmentValue as Guid?).Value);

            if (typeof(Guid).IsInstanceOfType(assignmentValue))
                return TableQuery.GenerateFilterConditionForGuid(assignmentName, queryComparison, (Guid)assignmentValue);

            if (typeof(bool).IsInstanceOfType(assignmentValue))
                return TableQuery.GenerateFilterConditionForBool(assignmentName, queryComparison, (bool)assignmentValue);

            if (typeof(DateTime).IsInstanceOfType(assignmentValue))
                return TableQuery.GenerateFilterConditionForDate(assignmentName, queryComparison, (DateTime)assignmentValue);

            if (typeof(int).IsInstanceOfType(assignmentValue))
                return TableQuery.GenerateFilterConditionForInt(assignmentName, queryComparison, (int)assignmentValue);

            if (typeof(string).IsInstanceOfType(assignmentValue))
                return TableQuery.GenerateFilterCondition(assignmentName, queryComparison, (string)assignmentValue);

            throw new NotImplementedException($"No filter condition created for type {assignmentValue.GetType().FullName}");
        }

        private static TableQuery<TableEntity<TEntity>> ResolveUnaryExpression<TEntity>(UnaryExpression expression, out Func<TEntity, bool> postFilter)
        {
            postFilter = (entity) => true;
            var operand = expression.Operand;
            if (!(operand is MemberExpression))
                throw new NotImplementedException($"Unary expression of type {operand.GetType().FullName} is not supported.");

            var memberOperand = operand as MemberExpression;
            var assignmentMember = ResolveMemberInType(typeof(TEntity), memberOperand);
            var assignmentName = assignmentMember.GetCustomAttribute<StoragePropertyAttribute>().Name;
            if (assignmentName.IsNullOrWhiteSpace())
                assignmentName = assignmentMember.Name;

            var query = new TableQuery<TableEntity<TEntity>>();
            var nullableHasValueProperty = typeof(Nullable<>).GetProperty("HasValue");
            if (memberOperand.Member == nullableHasValueProperty)
            {
                postFilter =
                        (entity) =>
                        {
                            var nullableValue = assignmentMember.GetValue(entity);
                            var hasValue = nullableHasValueProperty.GetValue(nullableValue);
                            var hasValueBool = (bool)hasValue;
                            return !hasValueBool;
                        };
                return query;

                if (expression.NodeType == ExpressionType.Not)
                {
                    var whereExpr = TableQuery.GenerateFilterCondition(assignmentName, QueryComparisons.Equal, "");
                    var whereQuery = query.Where(whereExpr);
                    return whereQuery;
                }
                {
                    var whereExpr = TableQuery.GenerateFilterCondition(assignmentName, QueryComparisons.NotEqual, "");
                    var whereQuery = query.Where(whereExpr);
                    return whereQuery;
                }
            }

            var refOptionalHasValueProperty = typeof(EastFive.IRefOptionalBase).GetProperty("HasValue");
            if (memberOperand.Member == refOptionalHasValueProperty)
            {
                postFilter =
                    (entity) =>
                    {
                        var nullableValue = assignmentMember.GetValue(entity);
                        var hasValue = refOptionalHasValueProperty.GetValue(nullableValue);
                        var hasValueBool = (bool)hasValue;
                        return !hasValueBool;
                    };
                return query;

                if (expression.NodeType == ExpressionType.Not)
                {
                    var whereExpr = TableQuery.GenerateFilterCondition(assignmentName, QueryComparisons.Equal, null);
                    var whereQuery = query.Where(whereExpr);
                    return whereQuery;
                }
                {
                    var whereExpr = TableQuery.GenerateFilterCondition(assignmentName, QueryComparisons.NotEqual, "");
                    var whereQuery = query.Where(whereExpr);
                    return whereQuery;
                }
            }

            throw new NotImplementedException($"Unary expression of type {memberOperand.Member.DeclaringType.FullName}..{memberOperand.Member.Name} is not supported.");
        }

        private static TableQuery<TableEntity<TEntity>> ResolveConstantExpression<TEntity>(ConstantExpression expression)
        {
            if (!typeof(bool).IsAssignableFrom(expression.Type))
                throw new NotImplementedException($"Constant expression of type {expression.Type.FullName} is not supported.");

            var value = (bool)expression.Value;
            if (!value)
                throw new Exception("Query for nothing?");

            var query = new TableQuery<TableEntity<TEntity>>();
            return query;
        }

        private static TableQuery<TableEntity<TEntity>> ResolveMemberExpression<TEntity>(MemberExpression expression)
        {
            var assignmentMember = ResolveMemberInType(typeof(TEntity), expression);
            if (!typeof(bool).IsAssignableFrom(expression.Type))
                throw new NotImplementedException($"Member expression of type {expression.Type.FullName} is not supported.");

            var query = new TableQuery<TableEntity<TEntity>>();
            var assignmentName = assignmentMember.GetCustomAttribute<StoragePropertyAttribute>().Name;
            if (assignmentName.IsNullOrWhiteSpace())
                assignmentName = assignmentMember.Name;
            var filter = TableQuery.GenerateFilterConditionForBool(assignmentName, QueryComparisons.Equal, true);
            var whereQuery = query.Where(filter);
            return whereQuery;
        }

        private static TableQuery<TableEntity<TEntity>> ResolveExpression<TEntity>(Expression<Func<TEntity, bool>> filter, out Func<TEntity, bool> postFilter)
        {
            if (filter.Body is UnaryExpression)
                return ResolveUnaryExpression<TEntity>(filter.Body as UnaryExpression, out postFilter);

            postFilter = (entity) => true;
            if (filter.Body is ConstantExpression)
                return ResolveConstantExpression<TEntity>(filter.Body as ConstantExpression);

            if (filter.Body is MemberExpression)
                return ResolveMemberExpression<TEntity>(filter.Body as MemberExpression);

            if (!(filter.Body is BinaryExpression))
                throw new ArgumentException("TableQuery expression is not a binary expression");

            var binaryExpression = filter.Body as BinaryExpression;
            if (!(binaryExpression.Left is MemberExpression))
                throw new ArgumentException("TableQuery expression left side must be an MemberExpression");

            var query = new TableQuery<TableEntity<TEntity>>();
            var assignmentMember = ResolveMemberInType(typeof(TEntity), binaryExpression.Left as MemberExpression);
            var assignmentValue = binaryExpression.Right.ResolveExpression();
            var assignmentName = assignmentMember.GetCustomAttribute<StoragePropertyAttribute>().Name;
            if (assignmentName.IsNullOrWhiteSpace())
                assignmentName = assignmentMember.Name;

            var whereExpr = WhereExpression(binaryExpression.NodeType, assignmentName, assignmentValue);
            var whereQuery = query.Where(whereExpr);
            return whereQuery;
        }

        public IEnumerableAsync<TEntity> FindAll<TEntity>(
            Expression<Func<TEntity, bool>> filter,
            int numberOfTimesToRetry = DefaultNumberOfTimesToRetry)
        {
            var table = TableFromEntity<TEntity>();
            var query = ResolveExpression(filter, out Func<TEntity, bool> postFilter);
            var token = default(TableContinuationToken);
            //var segment = default(TableQuerySegment<TableEntity<TEntity>>);
            var results = default(TEntity[]);
            var resultsIndex = 0;
            return EnumerableAsync.Yield<TEntity>(
                async (yieldReturn, yieldBreak) =>
                {
                    if (results.IsDefaultOrNull() || results.Length <= resultsIndex)
                    {
                        resultsIndex = 0;
                        while (true)
                        {
                            try
                            {
                                if ((!results.IsDefaultOrNull()) && token.IsDefaultOrNull())
                                    return yieldBreak;

                                var segment = await table.ExecuteQuerySegmentedAsync(query, token);
                                results = segment.Results.Select(segResult => segResult.Entity).Where(f => postFilter(f)).ToArray();
                                token = segment.ContinuationToken;
                                if (!segment.Results.Any())
                                    continue;
                                break;
                            }
                            catch (AggregateException)
                            {
                                throw;
                            }
                            catch (Exception ex)
                            {
                                if (!table.Exists())
                                    return yieldBreak;
                                if (ex is StorageException except && except.IsProblemTimeout())
                                {
                                    if (--numberOfTimesToRetry > 0)
                                    {
                                        await Task.Delay(DefaultBackoffForRetry);
                                        continue;
                                    }
                                }
                                throw;
                            }
                        }
                    }

                    var result = results[resultsIndex];
                    resultsIndex++;
                    return yieldReturn(result);
                });
        }

        public async Task<TResult> UpdateIfNotModifiedAsync<TData, TResult>(TData data,
            Func<TResult> success,
            Func<TResult> documentModified,
            Func<ExtendedErrorInformationCodes, string, TResult> onFailure = null,
            AzureStorageDriver.RetryDelegate onTimeout = null)
        {
            try
            {
                var table = GetTable<TData>();
                var tableData = TableEntity<TData>.Create(data);
                var update = TableOperation.Replace(tableData);
                await table.ExecuteAsync(update);
                return success();
            }
            catch (StorageException ex)
            {
                return await ex.ParseStorageException(
                    async (errorCode, errorMessage) =>
                    {
                        switch (errorCode)
                        {
                            case ExtendedErrorInformationCodes.Timeout:
                                {
                                    var timeoutResult = default(TResult);
                                    if (default(AzureStorageDriver.RetryDelegate) == onTimeout)
                                        onTimeout = AzureStorageDriver.GetRetryDelegate();
                                    await onTimeout(ex.RequestInformation.HttpStatusCode, ex,
                                        async () =>
                                        {
                                            timeoutResult = await UpdateIfNotModifiedAsync(data, success, documentModified, onFailure, onTimeout);
                                        });
                                    return timeoutResult;
                                }
                            case ExtendedErrorInformationCodes.UpdateConditionNotSatisfied:
                                {
                                    return documentModified();
                                }
                            default:
                                {
                                    if (onFailure.IsDefaultOrNull())
                                        throw ex;
                                    return onFailure(errorCode, errorMessage);
                                }
                        }
                    },
                    () =>
                    {
                        throw ex;
                    });
            }
        }

        #endregion

        #region CREATE


        #endregion

        #region Find
        
        public Task<TResult> FindByIdAsync<TEntity, TResult>(
                Guid rowKey,
            Func<TEntity, TResult> onSuccess,
            Func<TResult> onNotFound,
            Func<ExtendedErrorInformationCodes, string, TResult> onFailure =
                default(Func<ExtendedErrorInformationCodes, string, TResult>),
            AzureStorageDriver.RetryDelegate onTimeout =
                default(AzureStorageDriver.RetryDelegate))
        {
            return FindByIdAsync(rowKey.AsRowKey(), onSuccess, onNotFound, onFailure, onTimeout);
        }

        public Task<TResult> FindByIdAsync<TEntity, TResult>(
                string rowKey,
            Func<TEntity, TResult> onSuccess,
            Func<TResult> onNotFound,
            Func<ExtendedErrorInformationCodes, string, TResult> onFailure =
                default(Func<ExtendedErrorInformationCodes, string, TResult>),
            AzureStorageDriver.RetryDelegate onTimeout =
                default(AzureStorageDriver.RetryDelegate))
        {
            return FindByIdAsync(rowKey, rowKey.GeneratePartitionKey(),
                onSuccess, onNotFound, onFailure, onTimeout);
        }
        
        #endregion

        #region Update

        public async Task<TResult> UpdateAsync<TData, TResult>(Guid documentId,
            Func<TData, Func<TData, Task>, Task<TResult>> onUpdate,
            Func<TResult> onNotFound = default(Func<TResult>),
            AzureStorageDriver.RetryDelegateAsync<Task<TResult>> onTimeoutAsync =
                default(AzureStorageDriver.RetryDelegateAsync<Task<TResult>>))
        {
            var rowKey = documentId.AsRowKey();
            var partitionKey = rowKey.GeneratePartitionKey();
            return await UpdateAsync(rowKey, partitionKey, onUpdate, onNotFound);
        }

        public async Task<TResult> UpdateAsync<TData, TResult>(Guid documentId, string partitionKey,
            Func<TData, Func<TData, Task>, Task<TResult>> onUpdate,
            Func<TResult> onNotFound = default(Func<TResult>),
            AzureStorageDriver.RetryDelegateAsync<Task<TResult>> onTimeoutAsync =
                default(AzureStorageDriver.RetryDelegateAsync<Task<TResult>>))
        {
            var rowKey = documentId.AsRowKey();
            return await UpdateAsync(rowKey, partitionKey, onUpdate, onNotFound);
        }

        public Task<TResult> UpdateAsync<TData, TResult>(string rowKey, string partitionKey,
            Func<TData, Func<TData, Task>, Task<TResult>> onUpdate,
            Func<TResult> onNotFound = default(Func<TResult>),
            AzureStorageDriver.RetryDelegateAsync<Task<TResult>> onTimeoutAsync = 
                default(AzureStorageDriver.RetryDelegateAsync<Task<TResult>>))
        {
            return UpdateAsyncAsync(rowKey, partitionKey, onUpdate, onNotFound.AsAsyncFunc(), onTimeoutAsync);
        }

        public async Task<TResult> UpdateAsyncAsync<TData, TResult>(string rowKey, string partitionKey,
            Func<TData, Func<TData, Task>, Task<TResult>> onUpdate,
            Func<Task<TResult>> onNotFound = default(Func<Task<TResult>>),
            AzureStorageDriver.RetryDelegateAsync<Task<TResult>> onTimeoutAsync =
                default(AzureStorageDriver.RetryDelegateAsync<Task<TResult>>))
        {
            return await await FindByIdAsync(rowKey, partitionKey,
                async (TData currentStorage) =>
                {
                    var resultGlobal = default(TResult);
                    var useResultGlobal = false;
                    var resultLocal = await onUpdate.Invoke(currentStorage,
                        async (documentToSave) =>
                        {
                            useResultGlobal = await await UpdateIfNotModifiedAsync(documentToSave,
                                () =>
                                {
                                    return false.AsTask();
                                },
                                async () =>
                                {
                                    if (onTimeoutAsync.IsDefaultOrNull())
                                        onTimeoutAsync = AzureStorageDriver.GetRetryDelegateContentionAsync<Task<TResult>>();

                                    resultGlobal = await await onTimeoutAsync(
                                        async () => await UpdateAsyncAsync(rowKey, partitionKey, onUpdate, onNotFound, onTimeoutAsync),
                                        (numberOfRetries) => { throw new Exception("Failed to gain atomic access to document after " + numberOfRetries + " attempts"); });
                                    return true;
                                },
                                onTimeout: AzureStorageDriver.GetRetryDelegate());
                        });
                    return useResultGlobal ? resultGlobal : resultLocal;
                },
                onNotFound,
                default(Func<ExtendedErrorInformationCodes, string, Task<TResult>>),
                AzureStorageDriver.GetRetryDelegate());
        }

        #endregion


        public async Task<TResult> DeleteByIdAsync<TData, TResult>(Guid documentId,
            Func<TResult> success,
            Func<TResult> onNotFound,
            Func<ExtendedErrorInformationCodes, string, TResult> onFailure =
                default(Func<ExtendedErrorInformationCodes, string, TResult>),
            AzureStorageDriver.RetryDelegate onTimeout = default(AzureStorageDriver.RetryDelegate))
        {
            var table = GetTable<TData>();
            if (default(CloudTable) == table)
                return onNotFound();

            var document = DeletableEntity<TData>.Delete(documentId);
            var delete = TableOperation.Delete(document);
            try
            {
                await table.ExecuteAsync(delete);
                return success();
            }
            catch (StorageException se)
            {
                return await se.ParseStorageException(
                    async (errorCode, errorMessage) =>
                    {
                        switch (errorCode)
                        {
                            case ExtendedErrorInformationCodes.Timeout:
                                {
                                    var timeoutResult = default(TResult);
                                    if (default(AzureStorageDriver.RetryDelegate) == onTimeout)
                                        onTimeout = AzureStorageDriver.GetRetryDelegate();
                                    await onTimeout(se.RequestInformation.HttpStatusCode, se,
                                        async () =>
                                        {
                                            timeoutResult = await DeleteByIdAsync<TData, TResult>(documentId, success, onNotFound, onFailure, onTimeout);
                                        });
                                    return timeoutResult;
                                }
                            case ExtendedErrorInformationCodes.TableNotFound:
                            case ExtendedErrorInformationCodes.TableBeingDeleted:
                                {
                                    return onNotFound();
                                }
                            default:
                                {
                                    if (se.IsProblemDoesNotExist())
                                        return onNotFound();
                                    if (onFailure.IsDefaultOrNull())
                                        throw se;
                                    return onFailure(errorCode, errorMessage);
                                }
                        }
                    },
                    () =>
                    {
                        throw se;
                    });
            }
        }
        

        #region Locking

        public delegate Task<TResult> WhileLockedDelegateAsync<TDocument, TResult>(TDocument document,
            Func<Func<TDocument, Func<TDocument, Task>, Task>, Task> unlockAndSave,
            Func<Task> unlock);

        public delegate Task<TResult> ConditionForLockingDelegateAsync<TDocument, TResult>(TDocument document,
            Func<Task<TResult>> continueLocking);
        public delegate Task<TResult> ContinueAquiringLockDelegateAsync<TDocument, TResult>(int retryAttempts, TimeSpan elapsedTime,
                TDocument document,
            Func<Task<TResult>> continueAquiring,
            Func<Task<TResult>> force = default(Func<Task<TResult>>));

        public Task<TResult> LockedUpdateAsync<TDocument, TResult>(Guid id,
                Expression<Func<TDocument, DateTime?>> lockedPropertyExpression,
            WhileLockedDelegateAsync<TDocument, TResult> onLockAquired,
            Func<TResult> onNotFound,
            Func<TResult> onLockRejected = default(Func<TResult>),
                ContinueAquiringLockDelegateAsync<TDocument, TResult> onAlreadyLocked =
                        default(ContinueAquiringLockDelegateAsync<TDocument, TResult>),
                    ConditionForLockingDelegateAsync<TDocument, TResult> shouldLock =
                        default(ConditionForLockingDelegateAsync<TDocument, TResult>),
                AzureStorageDriver.RetryDelegateAsync<Task<TResult>> onTimeout = default(AzureStorageDriver.RetryDelegateAsync<Task<TResult>>),
                Func<TDocument, TDocument> mutateUponLock = default(Func<TDocument, TDocument>)) => LockedUpdateAsync(id, 
                    lockedPropertyExpression, 0, DateTime.UtcNow,
                onLockAquired,
                onNotFound,
                onLockRejected,
                onAlreadyLocked,
                shouldLock,
                onTimeout,
                mutateUponLock);

        private async Task<TResult> LockedUpdateAsync<TDocument, TResult>(Guid id,
                Expression<Func<TDocument, DateTime?>> lockedPropertyExpression,
                int retryCount,
                DateTime initialPass,
            WhileLockedDelegateAsync<TDocument, TResult> onLockAquired,
            Func<TResult> onNotFound,
            Func<TResult> onLockRejected = default(Func<TResult>),
                ContinueAquiringLockDelegateAsync<TDocument, TResult> onAlreadyLocked =
                    default(ContinueAquiringLockDelegateAsync<TDocument, TResult>),
                ConditionForLockingDelegateAsync<TDocument, TResult> shouldLock =
                    default(ConditionForLockingDelegateAsync<TDocument, TResult>),
                AzureStorageDriver.RetryDelegateAsync<Task<TResult>> onTimeout = default(AzureStorageDriver.RetryDelegateAsync<Task<TResult>>),
                Func<TDocument, TDocument> mutateUponLock = default(Func<TDocument, TDocument>))
        {
            if (onTimeout.IsDefaultOrNull())
                onTimeout = AzureStorageDriver.GetRetryDelegateContentionAsync<Task<TResult>>();

            if (onAlreadyLocked.IsDefaultOrNull())
                onAlreadyLocked = (retryCountDiscard, initialPassDiscard, doc, continueAquiring, force) => continueAquiring();

            if (onLockRejected.IsDefaultOrNull())
                if (!shouldLock.IsDefaultOrNull())
                    throw new ArgumentNullException("onLockRejected", "onLockRejected must be specified if shouldLock is specified");

            if (shouldLock.IsDefaultOrNull())
            {
                // both values 
                shouldLock = (doc, continueLocking) => continueLocking();
                onLockRejected = () => throw new Exception("shouldLock failed to continueLocking");
            }

            #region lock property expressions for easy use later

            var lockedPropertyMember = ((MemberExpression)lockedPropertyExpression.Body).Member;
            var fieldInfo = lockedPropertyMember as FieldInfo;
            var propertyInfo = lockedPropertyMember as PropertyInfo;

            bool isDocumentLocked(TDocument document)
            {
                var lockValueObj = fieldInfo != null ?
                    fieldInfo.GetValue(document)
                    :
                    propertyInfo.GetValue(document);
                var lockValue = (DateTime?)lockValueObj;
                var documentLocked = lockValue.HasValue;
                return documentLocked;
            }
            void lockDocument(TDocument document)
            {
                if (fieldInfo != null)
                    fieldInfo.SetValue(document, DateTime.UtcNow);
                else
                    propertyInfo.SetValue(document, DateTime.UtcNow);
            }
            void unlockDocument(TDocument documentLocked)
            {
                if (fieldInfo != null)
                    fieldInfo.SetValue(documentLocked, default(DateTime?));
                else
                    propertyInfo.SetValue(documentLocked, default(DateTime?));
            }

            // retryIncrease because some retries don't count
            Task<TResult> retry(int retryIncrease) => LockedUpdateAsync(id,
                    lockedPropertyExpression, retryCount + retryIncrease, initialPass,
                onLockAquired,
                onNotFound,
                onLockRejected,
                onAlreadyLocked,
                    shouldLock,
                    onTimeout);

            #endregion

            return await await this.FindByIdAsync(id,
                async (TDocument document) =>
                {
                    async Task<TResult> execute()
                    {
                        if (!mutateUponLock.IsDefaultOrNull())
                            document = mutateUponLock(document);
                        // Save document in locked state
                        return await await this.UpdateIfNotModifiedAsync(document,
                            () => PerformLockedCallback(id, document, unlockDocument, onLockAquired),
                            () => retry(0));
                    }

                    return await shouldLock(document,
                        () =>
                        {
                            #region Set document to locked state if not already locked

                            var documentLocked = isDocumentLocked(document);
                            if (documentLocked)
                            {
                                return onAlreadyLocked(retryCount,
                                        DateTime.UtcNow - initialPass, document,
                                    () => retry(1),
                                    () => execute());
                            }
                            lockDocument(document);

                            #endregion

                            return execute();
                        });
                },
                onNotFound.AsAsyncFunc());
            // TODO: onTimeout:onTimeout);
        }

        private async Task<TResult> PerformLockedCallback<TDocument, TResult>(
            Guid id,
            TDocument documentLocked,
            Action<TDocument> unlockDocument,
            WhileLockedDelegateAsync<TDocument, TResult> success)
        {
            try
            {
                var result = await success(documentLocked,
                    async (update) =>
                    {
                        var exists = await UpdateAsync<TDocument, bool>(id,
                            async (entityLocked, save) =>
                            {
                                await update(entityLocked,
                                    async (entityMutated) =>
                                    {
                                        unlockDocument(entityMutated);
                                        await save(entityMutated);
                                    });
                                return true;
                            },
                            () => false);
                    },
                    async () =>
                    {
                        var exists = await UpdateAsync<TDocument, bool>(id,
                            async (entityLocked, save) =>
                            {
                                unlockDocument(entityLocked);
                                await save(entityLocked);
                                return true;
                            },
                            () => false);
                    });
                return result;
            }
            catch (Exception)
            {
                var exists = await UpdateAsync<TDocument, bool>(id,
                    async (entityLocked, save) =>
                    {
                        unlockDocument(entityLocked);
                        await save(entityLocked);
                        return true;
                    },
                    () => false);
                throw;
            }
        }

        #endregion

    }
}

//public async Task<TResult> FindAllAsync<TEntity, TResult>(
//        Expression<Func<TEntity, bool>> filter,
//    Func<TEntity[], TResult> onSuccess,
//    Func<ExtendedErrorInformationCodes, string, TResult> onFailure =
//        default(Func<ExtendedErrorInformationCodes, string, TResult>),
//    AzureStorageDriver.RetryDelegate onTimeout =
//        default(AzureStorageDriver.RetryDelegate))
//{
//    var tableName = typeof(TEntity).GetCustomAttribute<TableEntityAttribute, string>(
//        attr => attr.TableName,
//        () => typeof(TEntity).Name);
//    var propertyNames = typeof(TEntity)
//        .GetProperties()
//        .Where(propInfo => propInfo.ContainsCustomAttribute<StoragePropertyAttribute>())
//        .Select(propInfo => propInfo.GetCustomAttribute<StoragePropertyAttribute>().Name)
//        .Join(",");

//    var http = new HttpClient(
//        new SharedKeySignatureStoreTablesMessageHandler(this.accountName, this.accountKey))
//    {
//        Timeout = new TimeSpan(0, 5, 0)
//    };

//    var filterAndParameter = filter.IsNullOrWhiteSpace(
//        () => string.Empty,
//        queryExpression => $"$filter={queryExpression}&");
//    var url = $"https://{accountName}.table.core.windows.net/{tableName}()?{filterAndParameter}$select=propertyNames";
//    // https://myaccount.table.core.windows.net/mytable()?$filter=<query-expression>&$select=<comma-separated-property-names>
//    var response = await http.GetAsync(url);

//    return onSuccess(default(TEntity).AsArray());
//}