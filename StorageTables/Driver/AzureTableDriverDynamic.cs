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
            var tableType = typeof(TEntity);
            if (tableType.IsSubClassOfGeneric(typeof(TableEntity<>)))
                tableType = tableType.GenericTypeArguments.First();

            var tableName = tableType.GetCustomAttribute<TableEntityAttribute, string>(
                attr => attr.TableName,
                () => tableType.Name);

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
            CloudTable table = default(CloudTable),
            AzureStorageDriver.RetryDelegate onTimeout =
                default(AzureStorageDriver.RetryDelegate))
        {
            var operation = TableOperation.Retrieve(partitionKey, rowKey,
                (string partitionKeyEntity, string rowKeyEntity, DateTimeOffset timestamp, IDictionary<string, EntityProperty> properties, string etag) =>
                {
                    var entityPopulated = TableEntity<TEntity>.CreateEntityInstance(properties);
                    return entityPopulated;
                });
            if (table.IsDefaultOrNull())
                table = TableFromEntity<TEntity>();
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
                                    table, onTimeout);
                        });
                    return result;
                }
                throw se;
            }

        }

        public IEnumerableAsync<TEntity> FindAll<TEntity>(
            TableQuery<TEntity> query,
            CloudTable table = default(CloudTable),
            int numberOfTimesToRetry = DefaultNumberOfTimesToRetry)
            where TEntity : ITableEntity, new()
        {
            if(table.IsDefaultOrNull())
                table = TableFromEntity<TEntity>();
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
                .Select(entity => DeletableEntity<TEntity>.Create(entity))
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
                table = TableFromEntity<TEntity>();

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

        public async Task<TResult> ReplaceAsync<TData, TResult>(TData data,
            Func<TResult> success,
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

        public Task<TResult> UpdateOrCreatesAsync<TData, TResult>(Guid documentId,
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
                            global = await this.UpdateOrCreatesAsync<TData, TResult>(documentId, onUpdate, onTimeoutAsync);
                            useGlobal = true;
                        });
                    if (useGlobal)
                        return global;
                    return result;
                });
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
                onSuccess, onNotFound, onFailure, table, onTimeout);
        }

        public IEnumerableAsync<TEntity> FindAll<TEntity>(
            Expression<Func<TEntity, bool>> filter,
            CloudTable table = default(CloudTable),
            int numberOfTimesToRetry = DefaultNumberOfTimesToRetry)
        {
            var query = filter.ResolveExpression(out Func<TEntity, bool> postFilter);
            return FindAll(query, table, numberOfTimesToRetry)
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
                default(CloudTable);
            string filter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKeyValue);
            var tableQuery = new TableQuery<TData>().Where(filter);
            return FindAll(tableQuery, table);
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
                                onTimeout: AzureStorageDriver.GetRetryDelegate());
                        });
                    return useResultGlobal ? resultGlobal : resultLocal;
                },
                onNotFound,
                default(Func<ExtendedErrorInformationCodes, string, Task<TResult>>),
                table,
                AzureStorageDriver.GetRetryDelegate());
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