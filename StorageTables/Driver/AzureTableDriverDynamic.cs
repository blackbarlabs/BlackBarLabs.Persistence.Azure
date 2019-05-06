using BlackBarLabs.Persistence.Azure;
using BlackBarLabs.Persistence.Azure.StorageTables;
using EastFive.Analytics;
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

        public readonly CloudTableClient TableClient;
        public readonly CloudBlobClient BlobClient;

        #region Init / Setup / Utility

        public AzureTableDriverDynamic(CloudStorageAccount storageAccount)
        {
            TableClient = storageAccount.CreateCloudTableClient();
            TableClient.DefaultRequestOptions.RetryPolicy =
                new ExponentialRetry(DefaultBackoffForRetry, DefaultNumberOfTimesToRetry);

            BlobClient = storageAccount.CreateCloudBlobClient();
            BlobClient.DefaultRequestOptions.RetryPolicy =
                new ExponentialRetry(DefaultBackoffForRetry, DefaultNumberOfTimesToRetry);
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

        private CloudTable GetTable<TEntity>()
        {
            var tableType = typeof(TEntity);
            return TableFromEntity(tableType, this.TableClient);
        }

        private static CloudTable TableFromEntity(Type tableType, CloudTableClient tableClient)
        {
            return tableType.GetAttributesInterface<IProvideTable>()
                .First(
                    (attr, next) => attr.GetTable(tableType, tableClient),
                    () =>
                    {
                        if (tableType.IsSubClassOfGeneric(typeof(TableEntity<>)))
                        {
                            var genericTableType = tableType.GenericTypeArguments.First();
                            return TableFromEntity(genericTableType, tableClient);
                        }
                        var tableName = tableType.Name.ToLower();
                        var table = tableClient.GetTableReference(tableName);
                        return table;
                    });
        }

        #endregion

        #region ITableEntity Management

        private static IAzureStorageTableEntity<TEntity> GetEntity<TEntity>(TEntity entity)
        {
            return typeof(TEntity)
                .GetAttributesInterface<IProvideEntity>()
                .First(
                    (entityProvider, next) =>
                    {
                        return entityProvider.GetEntity(entity);
                    },
                    () =>
                    {
                        return TableEntity<TEntity>.Create(entity);
                    });
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

        private class DeletableRPEntity<EntityType> : TableEntity<EntityType>
        {
            private string rowKey;
            private string partitionKey;

            public override string RowKey
            {
                get => rowKey;
                set { }
            }

            public override string PartitionKey
            {
                get => partitionKey;
                set { }
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

            internal static ITableEntity Delete(string rowKey, string partitionKey)
            {
                var deletableEntity = new DeletableRPEntity<EntityType>();
                deletableEntity.rowKey = rowKey;
                deletableEntity.partitionKey = partitionKey;
                return deletableEntity;
            }
        }

        #endregion

        #region Core

        public async Task<TResult> CreateAsync<TEntity, TResult>(TEntity entity,
            Func<IAzureStorageTableEntity<TEntity>, TResult> onSuccess,
            Func<TResult> onAlreadyExists,
            Func<ExtendedErrorInformationCodes, string, TResult> onFailure =
                default(Func<ExtendedErrorInformationCodes, string, TResult>),
            IHandleFailedModifications<TResult>[] onModificationFailures =
                default(IHandleFailedModifications<TResult>[]),
           AzureStorageDriver.RetryDelegate onTimeout = default(AzureStorageDriver.RetryDelegate),
           CloudTable table = default(CloudTable))
        {
            var tableEntity = GetEntity(entity);
            if(table.IsDefaultOrNull())
                table = GetTable<TEntity>();
            return await await tableEntity.ExecuteCreateModifiersAsync<Task<TResult>>(this,
                async rollback =>
                {
                    while (true)
                    {
                        try
                        {
                            var insert = TableOperation.Insert(tableEntity);
                            TableResult tableResult = await table.ExecuteAsync(insert);
                            return onSuccess(tableEntity);
                        }
                        catch (StorageException ex)
                        {
                            if (ex.IsProblemResourceAlreadyExists())
                            {
                                await rollback();
                                return onAlreadyExists();
                            }

                            var shouldRetry = await ex.ResolveCreate(table,
                                () => true,
                                onTimeout);
                            if (shouldRetry)
                                continue;

                            await rollback();
                            throw;
                        }
                        catch (Exception generalEx)
                        {
                            await rollback();
                            var message = generalEx;
                            throw;
                        }
                    }
                },
                (membersWithFailures) =>
                {
                    return onModificationFailures
                        .NullToEmpty()
                        .Where(
                            onModificationFailure =>
                            {
                                return onModificationFailure.DoesMatchMember(membersWithFailures);
                            })
                        .First<IHandleFailedModifications<TResult>, TResult>(
                            (onModificationFailure, next) => onModificationFailure.ModificationFailure(membersWithFailures),
                            () => throw new Exception("Modifiers failed to execute."))
                        .AsTask();
                });
            
        }

        public async Task<TResult> FindByIdAsync<TEntity, TResult>(
                string rowKey, string partitionKey,
            Func<TEntity, TResult> onSuccess,
            Func<TResult> onNotFound,
            Func<ExtendedErrorInformationCodes, string, TResult> onFailure =
                default(Func<ExtendedErrorInformationCodes, string, TResult>),
            CloudTable table = default(CloudTable),
            string tableName = default(string),
            AzureStorageDriver.RetryDelegate onTimeout =
                default(AzureStorageDriver.RetryDelegate))
        {
            var operation = TableOperation.Retrieve(partitionKey, rowKey,
                (string partitionKeyEntity, string rowKeyEntity, DateTimeOffset timestamp, IDictionary<string, EntityProperty> properties, string etag) =>
                {
                    return typeof(TEntity)
                        .GetAttributesInterface<IProvideEntity>()
                        .First(
                            (entityProvider, next) =>
                            {
                                var entityPopulated = entityProvider.CreateEntityInstance<TEntity>(
                                    rowKeyEntity, partitionKeyEntity, properties, etag, timestamp);
                                return entityPopulated;
                            },
                            () =>
                            {
                                var entityPopulated = TableEntity<TEntity>.CreateEntityInstance(properties);
                                return entityPopulated;
                            });
                });
            if (table.IsDefaultOrNull())
                table = tableName.HasBlackSpace()?
                    this.TableClient.GetTableReference(tableName)
                    :
                    table = GetTable<TEntity>();
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
                            result = await FindByIdAsync(rowKey, partitionKey,
                                onSuccess, onNotFound, onFailure,
                                    table:table, onTimeout:onTimeout);
                        });
                    return result;
                }
                throw se;
            }

        }

        public TResult FindBy<TRefEntity, TEntity, TResult>(IRef<TRefEntity> entityRef,
                Expression<Func<TEntity, IRef<TRefEntity>>> by,
            Func<IEnumerableAsync<TEntity>, TResult> onFound,
            Func<TResult> onRefNotFound = default(Func<TResult>),
            Func<ExtendedErrorInformationCodes, string, TResult> onFailure =
                default(Func<ExtendedErrorInformationCodes, string, TResult>))
            where TEntity : struct, IReferenceable
            where TRefEntity : struct, IReferenceable
        {
            return by.MemberInfo(
                memberInfo =>
                {
                    return memberInfo
                        .GetAttributesInterface<IProvideFindBy>()
                        .First<IProvideFindBy, TResult>(
                            (attr, next) =>
                            {
                                var results = attr
                                    .GetKeys(entityRef, this, memberInfo)
                                    .Select(
                                        rowParitionKeyKvp =>
                                        {
                                            var rowKey = rowParitionKeyKvp.Key;
                                            var partitionKey = rowParitionKeyKvp.Value;
                                            return this.FindByIdAsync(rowKey, partitionKey,
                                                (TEntity entity) => entity,
                                                () => default(TEntity?));
                                        })
                                    .Await()
                                    .SelectWhereHasValue();
                                return onFound(results);
                            },
                            () => throw new Exception());
                },
                () => throw new Exception());
        }

        public static IEnumerableAsync<TEntity> FindAllInternal<TEntity>(
            TableQuery<TEntity> query,
            CloudTable table,
            int numberOfTimesToRetry = DefaultNumberOfTimesToRetry)
            where TEntity : ITableEntity, new()
        {
            var token = default(TableContinuationToken);
            var segmentFecthing = table.ExecuteQuerySegmentedAsync(query, token);
            return EnumerableAsync.YieldBatch<TEntity>(
                async (yieldReturn, yieldBreak) =>
                {
                    if (segmentFecthing.IsDefaultOrNull())
                        return yieldBreak;
                    try
                    {
                        var segment = await segmentFecthing;
                        if (segment.IsDefaultOrNull())
                            return yieldBreak;

                        token = segment.ContinuationToken;
                        segmentFecthing = token.IsDefaultOrNull()?
                            default(Task<TableQuerySegment<TEntity>>)
                            :
                            table.ExecuteQuerySegmentedAsync(query, token);
                        var results = segment.Results.ToArray();
                        return yieldReturn(results);
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
                                segmentFecthing = token.IsDefaultOrNull() ?
                                    default(Task<TableQuerySegment<TEntity>>)
                                    :
                                    table.ExecuteQuerySegmentedAsync(query, token);
                                return yieldReturn(new TEntity[] { });
                            }
                        }
                        throw;
                    }
                });
        }

        public async Task<TableResult[]> CreateOrReplaceBatchAsync<TDocument>(string partitionKey, TDocument[] entities,
            CloudTable table = default(CloudTable),
                AzureStorageDriver.RetryDelegate onTimeout = default(AzureStorageDriver.RetryDelegate),
                EastFive.Analytics.ILogger diagnostics = default(EastFive.Analytics.ILogger))
            where TDocument : class, ITableEntity
        {
            if (!entities.Any())
                return new TableResult[] { };

            if(table.IsDefaultOrNull())
                table = GetTable<TDocument>();
            var bucketCount = (entities.Length / 100) + 1;
            diagnostics.Trace($"{entities.Length} rows for partition `{partitionKey}`.");

            var batch = new TableBatchOperation();
            var rowKeyHash = new HashSet<string>();
            foreach (var row in entities)
            {
                if (rowKeyHash.Contains(row.RowKey))
                {
                    diagnostics.Warning($"Duplicate rowkey `{row.RowKey}`.");
                    continue;
                }
                batch.InsertOrReplace(row);
            }

            // submit
            while (true)
            {
                try
                {
                    var resultList = await table.ExecuteBatchAsync(batch);
                    return resultList.ToArray();
                }
                catch (StorageException storageException)
                {
                    var shouldRetry = await storageException.ResolveCreate(table,
                        () => true,
                        onTimeout);
                    if (shouldRetry)
                        continue;

                }
            }
        }

        public IEnumerableAsync<TableResult> DeleteAll<TEntity>(
            Expression<Func<TEntity, bool>> filter,
            CloudTable table = default(CloudTable),
            int numberOfTimesToRetry = DefaultNumberOfTimesToRetry)
        {
            var finds = FindAll(filter, table, numberOfTimesToRetry);
            var deleted = finds
                .Select(entity => GetEntity(entity))
                .GroupBy(doc => doc.PartitionKey)
                .Select(
                    rowsToDeleteGrp =>
                    {
                        var partitionKey = rowsToDeleteGrp.Key;
                        var deletions = rowsToDeleteGrp
                            .Batch()
                            .Select(items => DeleteBatchAsync<TEntity>(partitionKey, items))
                            .Await()
                            .SelectMany();
                        return deletions;
                    })
               .SelectAsyncMany();
            return deleted;
        }

        private async Task<TableResult[]> DeleteBatchAsync<TEntity>(string partitionKey, ITableEntity[] entities,
            CloudTable table = default(CloudTable),
            EastFive.Analytics.ILogger diagnostics = default(EastFive.Analytics.ILogger))
        {
            if (!entities.Any())
                return new TableResult[] { };

            if(table.IsDefaultOrNull())
                table = GetTable<TEntity>();

            var batch = new TableBatchOperation();
            var rowKeyHash = new HashSet<string>();
            foreach (var row in entities)
            {
                if (rowKeyHash.Contains(row.RowKey))
                {
                    continue;
                }
                batch.Delete(row);
            }

            // submit
            while (true)
            {
                try
                {
                    var resultList = await table.ExecuteBatchAsync(batch);
                    return resultList.ToArray();
                }
                catch (StorageException storageException)
                {
                    if (storageException.IsProblemTableDoesNotExist())
                        return new TableResult[] { };
                    throw storageException;
                }
            }
        }

        private async Task<TResult> UpdateIfNotModifiedAsync<TData, TResult>(TData data,
                IAzureStorageTableEntity<TData> currentDocument,
            Func<TResult> success,
            Func<TResult> documentModified,
            Func<ExtendedErrorInformationCodes, string, TResult> onFailure = null,
            AzureStorageDriver.RetryDelegate onTimeout = null,
            CloudTable table = default(CloudTable))
        {
            if(table.IsDefaultOrNull())
                table = GetTable<TData>();
            var tableData = GetEntity(data);
            var update = TableOperation.Replace(tableData);
            var rollback = await tableData.ExecuteUpdateModifiersAsync(currentDocument, this,
                rollbacks => rollbacks,
                (members) => throw new Exception("Modifiers failed to execute."));
            try
            {
                await table.ExecuteAsync(update);
                return success();
            }
            catch (StorageException ex)
            {
                await rollback();
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
                                            timeoutResult = await UpdateIfNotModifiedAsync(data, currentDocument, success, documentModified, onFailure, onTimeout);
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

        public async Task<TResult> ReplaceAsync<TData, TResult>(TData data,
            Func<TResult> success,
            Func<ExtendedErrorInformationCodes, string, TResult> onFailure = null,
            AzureStorageDriver.RetryDelegate onTimeout = null)
        {
            var table = GetTable<TData>();
            var tableData = GetEntity(data);
            var update = TableOperation.Replace(tableData);
            var rollback = await tableData.ExecuteUpdateModifiersAsync(tableData, this,
                rollbacks => rollbacks,
                (members) => throw new Exception("Modifiers failed to execute."));
            try
            {
                await table.ExecuteAsync(update);
                return success();
            }
            catch (StorageException ex)
            {
                await rollback();
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
                                            timeoutResult = await ReplaceAsync(data, success, onFailure, onTimeout);
                                        });
                                    return timeoutResult;
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

        public Task<TResult> UpdateOrCreateAsync<TData, TResult>(Guid documentId,
            Func<bool, TData, Func<TData, Task>, Task<TResult>> onUpdate,
            AzureStorageDriver.RetryDelegateAsync<Task<TResult>> onTimeoutAsync =
                default(AzureStorageDriver.RetryDelegateAsync<Task<TResult>>))
        {
            return this.UpdateAsyncAsync<TData, TResult>(documentId,
                (doc, saveAsync) => onUpdate(false, doc, saveAsync),
                async () =>
                {
                    var doc = Activator.CreateInstance<TData>();
                    var global = default(TResult);
                    bool useGlobal = false;
                    var result = await onUpdate(true, doc,
                        async (docUpdated) =>
                        {
                        if (await this.CreateAsync(docUpdated,
                            discard => true,
                            () => false))
                            return;
                            global = await this.UpdateOrCreateAsync<TData, TResult>(documentId, onUpdate, onTimeoutAsync);
                            useGlobal = true;
                        });
                    if (useGlobal)
                        return global;
                    return result;
                });
        }

        public Task<TResult> UpdateOrCreateAsync<TData, TResult>(Guid documentId,
                Func<TData,TData> setId,
            Func<bool, TData, Func<TData, Task>, Task<TResult>> onUpdate,
                AzureStorageDriver.RetryDelegateAsync<Task<TResult>> onTimeoutAsync =
                    default(AzureStorageDriver.RetryDelegateAsync<Task<TResult>>))
        {
            return this.UpdateAsyncAsync<TData, TResult>(documentId,
                (doc, saveAsync) => onUpdate(false, doc, saveAsync),
                async () =>
                {
                    var doc = Activator.CreateInstance<TData>();
                    doc = setId(doc);
                    var global = default(TResult);
                    bool useGlobal = false;
                    var result = await onUpdate(true, doc,
                        async (docUpdated) =>
                        {
                            if (await this.CreateAsync(docUpdated,
                                discard => true,
                                () => false))
                                return;
                            global = await this.UpdateOrCreateAsync<TData, TResult>(documentId, setId, onUpdate, onTimeoutAsync);
                            useGlobal = true;
                        });
                    if (useGlobal)
                        return global;
                    return result;
                });
        }

        public Task<TResult> UpdateOrCreateAsync<TData, TResult>(string rowKey, string partitionKey,
            Func<bool, TData, Func<TData, Task>, Task<TResult>> onUpdate,
            AzureStorageDriver.RetryDelegateAsync<Task<TResult>> onTimeoutAsync =
                default(AzureStorageDriver.RetryDelegateAsync<Task<TResult>>),
            string tableName = default(string))
        {
            var table = default(CloudTable);
            if (tableName.HasBlackSpace())
                table = this.TableClient.GetTableReference(tableName);
            return this.UpdateAsyncAsync<TData, TResult>(rowKey, partitionKey,
                (doc, saveAsync) => onUpdate(false, doc, saveAsync),
                async () =>
                {
                    var doc = Activator.CreateInstance<TData>();
                    var global = default(TResult);
                    bool useGlobal = false;
                    var result = await onUpdate(true, doc,
                        async (docUpdated) =>
                        {
                            if (await this.CreateAsync(docUpdated,
                                discard => true,
                                () => false,
                                table:table))
                                return;
                            global = await this.UpdateOrCreateAsync<TData, TResult>(rowKey, partitionKey, onUpdate,
                                onTimeoutAsync:onTimeoutAsync, 
                                tableName:tableName);
                            useGlobal = true;
                        });
                    if (useGlobal)
                        return global;
                    return result;
                },
                table:table);
        }

        #endregion

        #region Find

        public Task<TResult> FindByIdAsync<TEntity, TResult>(
                Guid rowKey,
            Func<TEntity, TResult> onSuccess,
            Func<TResult> onNotFound,
            Func<ExtendedErrorInformationCodes, string, TResult> onFailure =
                default(Func<ExtendedErrorInformationCodes, string, TResult>),
            CloudTable table = default(CloudTable),
            AzureStorageDriver.RetryDelegate onTimeout =
                default(AzureStorageDriver.RetryDelegate))
        {
            return FindByIdAsync(rowKey.AsRowKey(), onSuccess, onNotFound, onFailure, table, onTimeout);
        }

        public Task<TResult> FindByIdAsync<TEntity, TResult>(
                string rowKey,
            Func<TEntity, TResult> onSuccess,
            Func<TResult> onNotFound,
            Func<ExtendedErrorInformationCodes, string, TResult> onFailure =
                default(Func<ExtendedErrorInformationCodes, string, TResult>),
            CloudTable table = default(CloudTable),
            AzureStorageDriver.RetryDelegate onTimeout =
                default(AzureStorageDriver.RetryDelegate))
        {
            return FindByIdAsync(rowKey, rowKey.GeneratePartitionKey(),
                onSuccess, onNotFound, onFailure, table:table, onTimeout:onTimeout);
        }

        public IEnumerableAsync<TEntity> FindByIdsAsync<TEntity>(
                Guid [] rowKeys,
            CloudTable table = default(CloudTable),
            AzureStorageDriver.RetryDelegate onTimeout =
                default(AzureStorageDriver.RetryDelegate))
            where TEntity : struct
        {
            if (table.IsDefaultOrNull())
                table = GetTable<TEntity>();
            return rowKeys
                .Select(
                    rowKey =>
                    {
                        return FindByIdAsync<TEntity, TEntity?>(rowKey,
                            (entity) => entity,
                            () => default(TEntity?),
                            table: table,
                            onTimeout: onTimeout);
                    })
                .AsyncEnumerable()
                .SelectWhereHasValue();
        }

        public IEnumerableAsync<TEntity> FindAll<TEntity>(
            Expression<Func<TEntity, bool>> filter,
            CloudTable table = default(CloudTable),
            int numberOfTimesToRetry = DefaultNumberOfTimesToRetry)
        {
            var query = filter.ResolveExpression(out Func<TEntity, bool> postFilter);
            var tableEntityTypes = query.GetType().GetGenericArguments();
            if (table.IsDefaultOrNull())
            {
                var tableEntityType = tableEntityTypes.First();
                if(tableEntityType.IsSubClassOfGeneric(typeof(IWrapTableEntity<>)))
                {
                    tableEntityType = tableEntityType.GetGenericArguments().First();
                }
                table = AzureTableDriverDynamic.TableFromEntity(tableEntityType, this.TableClient);
            }

            var findAllIntermediate = typeof(AzureTableDriverDynamic)
                .GetMethod("FindAllInternal", BindingFlags.Static | BindingFlags.Public)
                .MakeGenericMethod(tableEntityTypes)
                .Invoke(null, new object[] { query, table, numberOfTimesToRetry });
            var findAllCasted = findAllIntermediate as IEnumerableAsync<IWrapTableEntity<TEntity>>;
            return findAllCasted
                .Select(segResult => segResult.Entity)
                .Where(f => postFilter(f));
        }

        public IEnumerableAsync<TData> FindByPartition<TData>(string partitionKeyValue,
            string tableName = default(string))
            where TData  : ITableEntity, new()
        {
            var table = tableName.HasBlackSpace() ?
                this.TableClient.GetTableReference(tableName)
                :
                GetTable<TData>();
            string filter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKeyValue);
            var tableQuery = new TableQuery<TData>().Where(filter);
            return FindAllInternal(tableQuery, table);
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
            CloudTable table = default(CloudTable),
            AzureStorageDriver.RetryDelegateAsync<Task<TResult>> onTimeoutAsync = 
                default(AzureStorageDriver.RetryDelegateAsync<Task<TResult>>))
        {
            return UpdateAsyncAsync(rowKey, partitionKey,
                onUpdate, 
                onNotFound.AsAsyncFunc(),
                    table, onTimeoutAsync);
        }

        public async Task<TResult> UpdateAsyncAsync<TData, TResult>(Guid documentId,
            Func<TData, Func<TData, Task>, Task<TResult>> onUpdate,
            Func<Task<TResult>> onNotFound = default(Func<Task<TResult>>),
            AzureStorageDriver.RetryDelegateAsync<Task<TResult>> onTimeoutAsync =
                default(AzureStorageDriver.RetryDelegateAsync<Task<TResult>>))
        {
            var rowKey = documentId.AsRowKey();
            var partitionKey = rowKey.GeneratePartitionKey();
            return await UpdateAsyncAsync(rowKey, partitionKey, onUpdate, onNotFound);
        }

        public async Task<TResult> UpdateAsyncAsync<TData, TResult>(string rowKey, string partitionKey,
            Func<TData, Func<TData, Task>, Task<TResult>> onUpdate,
            Func<Task<TResult>> onNotFound = default(Func<Task<TResult>>),
            CloudTable table = default(CloudTable),
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
                                    GetEntity(currentStorage),
                                () =>
                                {
                                    return false.AsTask();
                                },
                                async () =>
                                {
                                    if (onTimeoutAsync.IsDefaultOrNull())
                                        onTimeoutAsync = AzureStorageDriver.GetRetryDelegateContentionAsync<Task<TResult>>();

                                    resultGlobal = await await onTimeoutAsync(
                                        async () => await UpdateAsyncAsync(rowKey, partitionKey, onUpdate, onNotFound, table, onTimeoutAsync),
                                        (numberOfRetries) => { throw new Exception("Failed to gain atomic access to document after " + numberOfRetries + " attempts"); });
                                    return true;
                                },
                                onTimeout: AzureStorageDriver.GetRetryDelegate(),
                                table:table);
                        });
                    return useResultGlobal ? resultGlobal : resultLocal;
                },
                onNotFound,
                default(Func<ExtendedErrorInformationCodes, string, Task<TResult>>),
                table:table,
                onTimeout:AzureStorageDriver.GetRetryDelegate());
        }

        #endregion

        #region Batch

        public IEnumerableAsync<TResult> CreateOrUpdateBatch<TResult>(IEnumerableAsync<ITableEntity> entities,
            Func<ITableEntity, TableResult, TResult> perItemCallback,
            string tableName = default(string),
            AzureStorageDriver.RetryDelegate onTimeout = default(AzureStorageDriver.RetryDelegate),
            EastFive.Analytics.ILogger diagnostics = default(EastFive.Analytics.ILogger))
        {
            return CreateOrReplaceBatch<ITableEntity, TResult>(entities,
                entity => entity.RowKey,
                entity => entity.PartitionKey,
                perItemCallback,
                tableName: tableName,
                onTimeout: onTimeout,
                diagnostics: diagnostics);
        }

        public IEnumerableAsync<TResult> CreateOrReplaceBatch<TDocument, TResult>(IEnumerableAsync<TDocument> entities,
                Func<TDocument, string> getRowKey,
                Func<TDocument, string> getPartitionKey,
                Func<ITableEntity, TableResult, TResult> perItemCallback,
                string tableName = default(string),
                AzureStorageDriver.RetryDelegate onTimeout = default(AzureStorageDriver.RetryDelegate),
                EastFive.Analytics.ILogger diagnostics = default(EastFive.Analytics.ILogger))
            where TDocument : class, ITableEntity
        {
            return entities
                .Batch()
                .Select(
                    rows =>
                    {
                        return CreateOrReplaceBatch(rows, getRowKey, getPartitionKey, perItemCallback, tableName, onTimeout);
                    })
                //.OnComplete(
                //    (resultss) =>
                //    {
                //        resultss.OnCompleteAll(
                //            resultsArray =>
                //            {
                //                if (tag.IsNullOrWhiteSpace())
                //                    return;

                //                if (!resultsArray.Any())
                //                    Console.WriteLine($"Batch[{tag}]:saved 0 {typeof(TDocument).Name} documents in 0 batches.");

                //                Console.WriteLine($"Batch[{tag}]:saved {resultsArray.Sum(results => results.Length)} {typeof(TDocument).Name} documents in {resultsArray.Length} batches.");
                //            });
                //    })
                .SelectAsyncMany();
        }

        public IEnumerableAsync<TResult> CreateOrUpdateBatch<TResult>(IEnumerable<ITableEntity> entities,
            Func<ITableEntity, TableResult, TResult> perItemCallback,
            string tableName = default(string),
            AzureStorageDriver.RetryDelegate onTimeout = default(AzureStorageDriver.RetryDelegate),
            EastFive.Analytics.ILogger diagnostics = default(EastFive.Analytics.ILogger))
        {
            return CreateOrReplaceBatch<ITableEntity, TResult>(entities,
                entity => entity.RowKey,
                entity => entity.PartitionKey,
                perItemCallback,
                tableName:tableName,
                onTimeout: onTimeout,
                diagnostics: diagnostics);
        }

        public IEnumerableAsync<TResult> CreateOrReplaceBatch<TDocument, TResult>(IEnumerable<TDocument> entities,
                Func<TDocument, string> getRowKey,
                Func<TDocument, string> getPartitionKey,
                Func<TDocument, TableResult, TResult> perItemCallback,
                string tableName = default(string),
                AzureStorageDriver.RetryDelegate onTimeout = default(AzureStorageDriver.RetryDelegate),
                EastFive.Analytics.ILogger diagnostics = default(EastFive.Analytics.ILogger))
            where TDocument : class, ITableEntity
        {
            var table = tableName.HasBlackSpace() ?
                TableClient.GetTableReference(tableName)
                :
                default(CloudTable);
            return entities
                .Select(
                    row =>
                    {
                        row.RowKey = getRowKey(row);
                        row.PartitionKey = getPartitionKey(row);
                        return row;
                    })
                .GroupBy(row => row.PartitionKey)
                .SelectMany(
                    grp =>
                    {
                        return grp
                            .Split(index => 100)
                            .Select(set => set.ToArray().PairWithKey(grp.Key));
                    })
                .Select(grp => CreateOrReplaceBatchAsync(grp.Key, grp.Value, table:table))
                .AsyncEnumerable()
                .OnComplete(
                    (resultss) =>
                    {
                        if (!resultss.Any())
                            diagnostics.Trace($"saved 0 {typeof(TDocument).Name} documents across 0 partitions.");

                        diagnostics.Trace($"saved {resultss.Sum(results => results.Length)} {typeof(TDocument).Name} documents across {resultss.Length} partitions.");
                    })
                .SelectMany(
                    trs =>
                    {
                        return trs
                            .Select(
                                tableResult =>
                                {
                                    var resultDocument = (tableResult.Result as TDocument);
                                    return perItemCallback(resultDocument, tableResult);
                                });
                    });
        }

        #endregion

        #region DELETE

        public Task<TResult> DeleteByIdAsync<TData, TResult>(Guid documentId,
            Func<TResult> success,
            Func<TResult> onNotFound,
            Func<ExtendedErrorInformationCodes, string, TResult> onFailure =
                default(Func<ExtendedErrorInformationCodes, string, TResult>),
            AzureStorageDriver.RetryDelegate onTimeout = default(AzureStorageDriver.RetryDelegate))
        {
            var rowKey = documentId.AsRowKey();
            var partitionKey = rowKey.GeneratePartitionKey();
            return DeleteAsync<TData, TResult>(rowKey, partitionKey,
                success,
                onNotFound,
                onFailure,
                onTimeout);
        }

        public async Task<TResult> DeleteAsync<TData, TResult>(string rowKey, string partitionKey,
            Func<TResult> success,
            Func<TResult> onNotFound,
            Func<ExtendedErrorInformationCodes, string, TResult> onFailure =
                default(Func<ExtendedErrorInformationCodes, string, TResult>),
            AzureStorageDriver.RetryDelegate onTimeout = default(AzureStorageDriver.RetryDelegate),
            string tableName = default(string))
        {
            return await await FindByIdAsync(rowKey, partitionKey,
                (TData data) =>
                {
                    var entity = GetEntity(data);
                    return DeleteAsync<TData, TResult>(entity,
                        success,
                        onNotFound,
                        onFailure,
                        onTimeout,
                        tableName: tableName);
                },
                onNotFound.AsAsyncFunc(),
                onTimeout: onTimeout,
                tableName: tableName);
        }

        public async Task<TResult> DeleteAsync<TData, TResult>(IAzureStorageTableEntity<TData> entity,
            Func<TResult> success,
            Func<TResult> onNotFound,
            Func<ExtendedErrorInformationCodes, string, TResult> onFailure =
                default(Func<ExtendedErrorInformationCodes, string, TResult>),
            AzureStorageDriver.RetryDelegate onTimeout = default(AzureStorageDriver.RetryDelegate),
            string tableName = default(string))
        {
            var table = tableName.HasBlackSpace() ?
                this.TableClient.GetTableReference(tableName)
                :
                GetTable<TData>();

            if (default(CloudTable) == table)
                return onNotFound();

            var rollback = await entity.ExecuteDeleteModifiersAsync(this,
                rb => rb,
                (modifiers) => throw new Exception("Modifiers failed to execute on delete."));
            var delete = TableOperation.Delete(entity);
            try
            {
                await table.ExecuteAsync(delete);
                return success();
            }
            catch (StorageException se)
            {
                await rollback();
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
                                            timeoutResult = await DeleteAsync<TData, TResult>(entity, success, onNotFound, onFailure, onTimeout);
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

        public IEnumerableAsync<TResult> DeleteBatch<TData, TResult>(IEnumerableAsync<Guid> documentIds,
            Func<TableResult, TResult> result,
            AzureStorageDriver.RetryDelegate onTimeout = default(AzureStorageDriver.RetryDelegate))
        {
            return documentIds
                .Select(subsetId => DeletableEntity<TData>.Delete(subsetId))
                .Batch()
                .Select(
                    docs =>
                    {
                        return docs
                            .GroupBy(doc => doc.PartitionKey)
                            .Select(
                                async partitionDocsGrp =>
                                {
                                    var results = await this.DeleteBatchAsync<TData>(partitionDocsGrp.Key, partitionDocsGrp.ToArray());
                                    return results.Select(tr => result(tr));
                                })
                            .AsyncEnumerable()
                            .SelectMany();
                    })
                .SelectAsyncMany();
        }

        #endregion

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
                onNotFound.AsAsyncFunc(),
                onLockRejected,
                onAlreadyLocked,
                shouldLock,
                onTimeout,
                mutateUponLock);

        public Task<TResult> LockedUpdateAsync<TDocument, TResult>(Guid id,
                Expression<Func<TDocument, DateTime?>> lockedPropertyExpression,
            WhileLockedDelegateAsync<TDocument, TResult> onLockAquired,
            Func<Task<TResult>> onNotFound,
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
            Func<Task<TResult>> onNotFoundAsync,
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
                onNotFoundAsync,
                onLockRejected,
                onAlreadyLocked,
                    shouldLock,
                    onTimeout);

            #endregion

            return await await this.FindByIdAsync(id,
                async (TDocument document) =>
                {
                    var originalDoc = GetEntity(document); // Not a deep, or even shallow, copy in most cases
                    async Task<TResult> execute()
                    {
                        if (!mutateUponLock.IsDefaultOrNull())
                            document = mutateUponLock(document);
                        // Save document in locked state
                        return await await this.UpdateIfNotModifiedAsync(document,
                                originalDoc, // should not be triggering modifers here anyway
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
                onNotFoundAsync);
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

