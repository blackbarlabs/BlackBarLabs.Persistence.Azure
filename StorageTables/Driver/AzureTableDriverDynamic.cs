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

        private CloudTable GetTable<T>()
        {
            var tableName = typeof(T).Name.ToLower();
            return TableClient.GetTableReference(tableName);
        }

        private class CreatableEntity : ITableEntity
        {
            private Type entityType;
            private object entity;

            public string RowKey
            {
                get
                {
                    var properties = entityType
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

                    var rowKeyValue = rowKeyProperty.GetValue(entity);
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
                    var partitionKey = RowKey.GeneratePartitionKey();// partitionKey;
                    return partitionKey;
                }
                set
                {
                    var x = value.GetType();

                }
            }

            public DateTimeOffset Timestamp { get; set; }

            public string ETag { get; set; }

            public void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
            {
                throw new NotImplementedException();
            }


            public IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
            {
                var valuesToStore = entityType
                    .GetMembers()
                    .Where(propInfo => propInfo.ContainsAttributeInterface<IPersistInAzureStorageTables>())
                    .SelectMany(
                        (propInfo) =>
                        {
                            var attrs = propInfo.GetAttributesInterface<IPersistInAzureStorageTables>();
                            if (attrs.Length > 1)
                            {
                                var propIdentifier = $"{propInfo.DeclaringType.FullName}__{propInfo.Name}";
                                var attributesInConflict = attrs.Select(a => a.GetType().FullName).Join(",");
                                throw new Exception($"{propIdentifier} has multiple IPersistInAzureStorageTables attributes:{attributesInConflict}.");
                            }
                            var attr = attrs.First() as IPersistInAzureStorageTables;
                            var value = propInfo.GetValue(this.entity);
                            return attr.ConvertValue(value, propInfo);
                        })
                    .ToDictionary();
                return valuesToStore;
            }

            internal static ITableEntity Create<TEntity>(TEntity entity)
            {
                var creatableEntity = new CreatableEntity();
                creatableEntity.entity = entity;
                creatableEntity.entityType = typeof(TEntity);
                return creatableEntity;
            }
        }

        private class TableEntity<EntityType> : ITableEntity
        {
            public EntityType Entity { get; private set; }

            public string RowKey
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
                    var partitionKey = RowKey.GeneratePartitionKey();// partitionKey;
                    return partitionKey;
                }
                set
                {
                    var x = value.GetType();

                }
            }

            public DateTimeOffset Timestamp { get; set; }

            public string ETag { get; set; }

            private IEnumerable<KeyValuePair<MemberInfo, IPersistInAzureStorageTables>> StorageProperties
            {
                get
                {
                    return typeof(EntityType)
                        .GetMembers()
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
            }
            
            public void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
            {
                this.Entity = Activator.CreateInstance<EntityType>();
                foreach (var propInfoAttribute in StorageProperties)
                {
                    var propInfo = propInfoAttribute.Key;
                    var attr = propInfoAttribute.Value;
                    var value = propInfo.GetValue(this.Entity);
                    attr.PopulateValue(value, propInfo, properties);
                }
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
                return creatableEntity;
            }
        }


        public async Task<TResult> CreateAsync<TEntity, TResult>(TEntity entity,
            Func<Guid, TResult> onSuccess,
            Func<TResult> onAlreadyExists,
            Func<ExtendedErrorInformationCodes, string, TResult> onFailure =
                default(Func<ExtendedErrorInformationCodes, string, TResult>),
           AzureStorageDriver.RetryDelegate onTimeout = default(AzureStorageDriver.RetryDelegate))
        {
            var tableEntity = CreatableEntity.Create(entity);
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

        private CloudTable TableFromEntity<TEntity>()
        {
            var tableName = typeof(TEntity).GetCustomAttribute<TableEntityAttribute, string>(
                attr => attr.TableName,
                () => typeof(TEntity).Name);
            
            var table = TableClient.GetTableReference(tableName);
            return table;
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
                    var entityInitial = Activator.CreateInstance<TEntity>();
                    var entityPopulated = typeof(TEntity)
                        .GetMembers()
                        .Where(propInfo => propInfo.ContainsCustomAttribute<StoragePropertyAttribute>())
                        .Aggregate(entityInitial,
                            (entity, propInfo) =>
                            {
                                var attr = propInfo.GetCustomAttribute<StoragePropertyAttribute>();
                                var key = attr.Name;
                                if (key.IsNullOrWhiteSpace())
                                    key = propInfo.Name;
                                if (!properties.ContainsKey(key))
                                    return entity;
                                var value = properties[key];
                                var type = propInfo.GetMemberType();
                                TEntity Select(object convertedValue)
                                {
                                    propInfo.SetValue(ref entity, convertedValue);
                                    return entity;
                                }

                                if (type.IsAssignableFrom(typeof(Guid)))
                                    return Select(value.GuidValue);

                                if (type.IsAssignableFrom(typeof(Guid[])))
                                {
                                    var guidsValue = value.BinaryValue.ToGuidsFromByteArray();
                                    return Select(guidsValue);
                                }

                                if (type.IsSubClassOfGeneric(typeof(IRef<>)))
                                {
                                    var guidValue = value.GuidValue;
                                    var resourceType = type.GenericTypeArguments.First();
                                    var instantiatableType = typeof(EastFive.Azure.Persistence.Ref<>).MakeGenericType(resourceType);
                                    var instance = Activator.CreateInstance(instantiatableType, new object[] { guidValue });
                                    return Select(instance);
                                }

                                if (type.IsAssignableFrom(typeof(string)))
                                    return Select(value.StringValue);

                                return entity;
                            });

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

        public IEnumerableAsync<TEntity> FindAll<TEntity>(
            Expression<Func<TEntity, bool>> filter,
            int numberOfTimesToRetry = DefaultNumberOfTimesToRetry)
        {
            var table = TableFromEntity<TableEntity<TEntity>>();
            var query = new TableQuery<TableEntity<TEntity>>();
            var token = default(TableContinuationToken);
            var segment = default(TableQuerySegment<TableEntity<TEntity>>);
            var resultsIndex = 0;
            return EnumerableAsync.Yield<TEntity>(
                async (yieldReturn, yieldBreak) =>
                {
                    if (segment.IsDefaultOrNull() || segment.Results.Count <= resultsIndex)
                    {
                        resultsIndex = 0;
                        while (true)
                        {
                            try
                            {
                                if ((!segment.IsDefaultOrNull()) && token.IsDefaultOrNull())
                                    return yieldBreak;

                                segment = await table.ExecuteQuerySegmentedAsync(query, token);
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

                    var result = segment.Results[resultsIndex];
                    resultsIndex++;
                    return yieldReturn(result.Entity);
                });
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


        private class DeletableEntity : ITableEntity
        {
            private Guid rowKeyValue;

            public string RowKey
            {
                get
                {
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
                    var partitionKey = RowKey.GeneratePartitionKey();// partitionKey;
                    return partitionKey;
                }
                set
                {
                    var x = value.GetType();

                }
            }

            public DateTimeOffset Timestamp { get; set; }

            public string ETag
            {
                get
                {
                    return "*";
                }
                set
                {
                }
            }

            public void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
            {
                throw new NotImplementedException();
            }

            public IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
            {
                throw new NotImplementedException();
            }

            internal static ITableEntity Delete(Guid rowKey)
            {
                var creatableEntity = new DeletableEntity();
                creatableEntity.rowKeyValue = rowKey;
                return creatableEntity;
            }
        }

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

            var document = DeletableEntity.Delete(documentId);
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


    }
}
