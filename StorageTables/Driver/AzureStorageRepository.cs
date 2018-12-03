using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Microsoft.WindowsAzure.Storage.Table;
using BlackBarLabs.Extensions;
using BlackBarLabs.Linq;
using EastFive;
using EastFive.Linq;
using EastFive.Extensions;
using EastFive.Azure.StorageTables.Driver;
using EastFive.Linq.Async;
using BlackBarLabs.Linq.Async;
using Microsoft.WindowsAzure.Storage.Blob;
using System.IO;
using EastFive.Analytics;

namespace BlackBarLabs.Persistence.Azure.StorageTables
{
    public partial class AzureStorageRepository : EastFive.Azure.StorageTables.Driver.AzureStorageDriver
    {
        public readonly CloudTableClient TableClient;
        public readonly CloudBlobClient BlobClient;
        private const int retryHttpStatus = 200;

        private readonly Exception retryException = new Exception();

        public AzureStorageRepository(CloudStorageAccount storageAccount)
        {
            TableClient = storageAccount.CreateCloudTableClient();
            TableClient.DefaultRequestOptions.RetryPolicy = retryPolicy;

            BlobClient = storageAccount.CreateCloudBlobClient();
            BlobClient.DefaultRequestOptions.RetryPolicy = retryPolicy;
        }

        public static AzureStorageRepository CreateRepository(
            string storageSettingConfigurationKeyName)
        {
            var storageSetting = Microsoft.Azure.CloudConfigurationManager.GetSetting(storageSettingConfigurationKeyName);
            var cloudStorageAccount = CloudStorageAccount.Parse(storageSetting);
            var azureStorageRepository = new AzureStorageRepository(cloudStorageAccount);
            return azureStorageRepository;
        }

        #region Blob methods

        public async Task<TResult> SaveBlobIfNotExistsAsync<TResult>(string containerReference,
                Guid id, byte[] data, Dictionary<string, string> metadata, string contentType,
            Func<TResult> success,
            Func<TResult> blobAlreadyExists,
            Func<string, TResult> failure)
        {
            try
            {
                var blockId = id.AsRowKey();
                var container = BlobClient.GetContainerReference(containerReference);
                if (!container.Exists())
                    return await SaveBlobAsync(containerReference, id, data, new Dictionary<string, string>(), contentType, success, failure);

                var blockBlob = container.GetBlockBlobReference(blockId);
                if (blockBlob.Exists())
                    return blobAlreadyExists();

                foreach (var item in metadata)
                {
                    blockBlob.Metadata[item.Key] = item.Value;
                }
                if (metadata.Count > 0)
                    await blockBlob.SetMetadataAsync();

                return await SaveBlobAsync(containerReference, id, data, new Dictionary<string, string>(), contentType, success, failure);
            }
            catch (Exception ex)
            {
                return failure(ex.Message);
            }
        }

        public async Task<TResult> SaveBlobAsync<TResult>(string containerReference, Guid id, byte[] data,
                Dictionary<string, string> metadata, string contentType,
            Func<TResult> success,
            Func<string, TResult> failure)
        {
            try
            {
                var blockId = id.AsRowKey();
                var container = BlobClient.GetContainerReference(containerReference);
                container.CreateIfNotExists();
                var blockBlob = container.GetBlockBlobReference(blockId);

                await blockBlob.UploadFromByteArrayAsync(data, 0, data.Length);

                foreach (var item in metadata)
                {
                    blockBlob.Metadata[item.Key] = item.Value;
                }
                if (metadata.Count > 0)
                    await blockBlob.SetMetadataAsync();

                if (!string.IsNullOrEmpty(contentType))
                {
                    blockBlob.Properties.ContentType = contentType;
                    await blockBlob.SetPropertiesAsync();
                }

                return success();
            }
            catch (Exception ex)
            {
                return failure(ex.Message);
            }
        }

        public async Task<TResult> ReadBlobAsync<TResult>(string containerReference, Guid id,
            Func<Stream, string, IDictionary<string, string>, TResult> success,
            Func<TResult> onNotFound,
            Func<string, TResult> failure)
        {
            try
            {
                var container = BlobClient.GetContainerReference(containerReference);
                bool created = await container.CreateIfNotExistsAsync();
                var blockId = id.AsRowKey();
                var blob = await container.GetBlobReferenceFromServerAsync(blockId);
                var returnStream = await blob.OpenReadAsync();
                return success(returnStream, blob.Properties.ContentType, blob.Metadata);
            }
            catch (StorageException storageEx)
            {
                return storageEx.ParseExtendedErrorInformation(
                    (errorCodes, reason) =>
                    {
                        return failure(reason);
                    },
                    () =>
                    {
                        return failure(storageEx.Message);
                    });
            }
            catch (Exception ex)
            {
                return failure(ex.Message);
            }
        }

        public async Task<TResult> ReadBlobMetadataAsync<TResult>(string containerReference, Guid id,
            Func<string, IDictionary<string, string>, TResult> success,
            Func<string, TResult> failure)
        {
            try
            {
                var container = BlobClient.GetContainerReference(containerReference);
                var blockId = id.AsRowKey();
                var blob = await container.GetBlobReferenceFromServerAsync(blockId);
                return success(blob.Properties.ContentType, blob.Metadata);
            }
            catch (Exception ex)
            {
                return failure(ex.Message);
            }
        }

        #endregion

        #region Table core methods

        private CloudTable GetTable<T>()
        {
            var tableName = typeof(T).Name.ToLower();
            return TableClient.GetTableReference(tableName);
        }

        public async Task DeleteTableAsync<T>()
        {
            try
            {
                var table = GetTable<T>();
                await table.DeleteAsync();
            }
            catch (StorageException ex)
            {
                if (!ex.IsProblemTableDoesNotExist())
                    throw;
            }
        }

        #endregion

        #region Direct methods
        
        public TResult FindByIdBatch<TDocument, TResult>(IEnumerableAsync<Guid> entityIds,
            Func<IEnumerableAsync<TDocument>, IEnumerableAsync<Guid>, TResult> onComplete,
            EastFive.Analytics.ILogger diagnosticsTag = default(EastFive.Analytics.ILogger))
        {
            var table = GetTable<TDocument>();
            var diagnosticsBatch = diagnosticsTag.CreateScope("Batch");
            var diagnosticsSelect = diagnosticsTag.CreateScope("Select");
            var results = entityIds
                .Batch(diagnosticsBatch)
                .Select(
                    entityIdSet =>
                    {
                        var total = entityIdSet.Length;
                        return entityIdSet;
                    },
                    diagnosticsSelect)
                .Select(
                    async entityIdSet =>
                    {
                        if (!entityIdSet.Any())
                            return new KeyValuePair<Guid, TableResult>[] { };

                        var batch = new TableBatchOperation();

                        foreach (var entityId in entityIdSet)
                        {
                            var rowKey = entityId.AsRowKey();
                            var paritionKey = rowKey.GeneratePartitionKey();
                            batch.Add(TableOperation.Retrieve(rowKey, paritionKey));
                        }

                        // submit
                        try
                        {
                            var resultList = await table.ExecuteBatchAsync(batch);
                            return entityIdSet
                                .Zip(resultList, (entityId, result) => entityId.PairWithValue(result))
                                .ToArray();
                        }
                        catch (StorageException storageException)
                        {
                            if (storageException.RequestInformation.ExtendedErrorInformation.ErrorCode == "InvalidDuplicateRow")
                                return new KeyValuePair<Guid, TableResult>[] { };
                            return new KeyValuePair<Guid, TableResult>[] { };
                        }
                    })
                .Await()
                .SelectMany();

            var resultsSuccess = results
                .Where(result => result.Value.HttpStatusCode < 400)
                .Select(
                    result =>
                    {
                        return (TDocument)result.Value.Result;
                    });

            var resultsFailure = results
                .Where(result => result.Value.HttpStatusCode >= 400)
                .Select(
                    result =>
                    {
                        return result.Key;
                    });

            return onComplete(resultsSuccess, resultsFailure);
        }

        public async Task<TResult> CreateOrReplaceBatchAsync<TDocument, TResult>(TDocument[] entities,
                Func<TDocument, Guid> getRowKey,
                Func<Guid[], Guid[], TResult> onSaved,
                RetryDelegate onTimeout = default(RetryDelegate))
            where TDocument : class, ITableEntity
        {
            var tableResults = await entities.Select(
                row =>
                {
                    row.RowKey = getRowKey(row).AsRowKey();
                    row.PartitionKey = row.RowKey.GeneratePartitionKey();
                    return row;
                })
                .GroupBy(row => row.PartitionKey)
                .Select(
                    grp => CreateOrReplaceBatchAsync(grp.Key, grp.ToArray()))
                .WhenAllAsync()
                .SelectManyAsync()
                .ToArrayAsync();

            return onSaved(
                tableResults
                    .Where(tr => tr.HttpStatusCode < 400)
                    .Select(
                        tr =>
                        {
                            var trS = (tr.Result as TDocument).RowKey;
                            return Guid.Parse(trS);
                        })
                    .ToArray(),
                tableResults
                    .Where(tr => tr.HttpStatusCode >= 400)
                    .Select(
                        tr =>
                        {
                            var trS = (tr.Result as TDocument).RowKey;
                            return Guid.Parse(trS);
                        })
                    .ToArray());
        }
        
        public IEnumerableAsync<TResult> CreateOrReplaceBatch<TDocument, TResult>(IEnumerableAsync<TDocument> entities,
                Func<TDocument, Guid> getRowKey,
                Func<TDocument, TResult> onSuccess,
                Func<TDocument, TResult> onFailure,
                RetryDelegate onTimeout = default(RetryDelegate),
                string tag = default(string))
            where TDocument : class, ITableEntity
        {
            return entities
                .Batch()
                .Select(
                    rows =>
                    {
                        return CreateOrReplaceBatch(rows, getRowKey, onSuccess, onFailure, onTimeout);
                    })
                .OnComplete(
                    (resultss) =>
                    {
                        resultss.OnCompleteAll(
                            resultsArray =>
                            {
                                if (tag.IsNullOrWhiteSpace())
                                    return;

                                if (!resultsArray.Any())
                                    Console.WriteLine($"Batch[{tag}]:saved 0 {typeof(TDocument).Name} documents in 0 batches.");

                                Console.WriteLine($"Batch[{tag}]:saved {resultsArray.Sum(results => results.Length)} {typeof(TDocument).Name} documents in {resultsArray.Length} batches.");
                            });
                    })
                .SelectAsyncMany();
        }

        public IEnumerableAsync<TResult> CreateOrReplaceBatch<TDocument, TResult>(IEnumerableAsync<TDocument> entities,
                Func<TDocument, string> getRowKey,
                Func<TDocument, string> getPartitionKey,
                Func<TDocument, TResult> onSuccess,
                Func<TDocument, TResult> onFailure,
                RetryDelegate onTimeout = default(RetryDelegate),
                string tag = default(string))
            where TDocument : class, ITableEntity
        {
            return entities
                .Batch()
                .Select(
                    rows =>
                    {
                        return CreateOrReplaceBatch(rows, getRowKey, getPartitionKey, onSuccess, onFailure, onTimeout);
                    })
                .OnComplete(
                    (resultss) =>
                    {
                        resultss.OnCompleteAll(
                            resultsArray =>
                            {
                                if (tag.IsNullOrWhiteSpace())
                                    return;

                                if (!resultsArray.Any())
                                    Console.WriteLine($"Batch[{tag}]:saved 0 {typeof(TDocument).Name} documents in 0 batches.");

                                Console.WriteLine($"Batch[{tag}]:saved {resultsArray.Sum(results => results.Length)} {typeof(TDocument).Name} documents in {resultsArray.Length} batches.");
                            });
                    })
                .SelectAsyncMany();
        }

        public IEnumerableAsync<TResult> CreateOrReplaceBatch<TDocument, TResult>(IEnumerable<TDocument> entities,
                Func<TDocument, Guid> getRowKey,
                Func<TDocument, TResult> onSuccess,
                Func<TDocument, TResult> onFailure,
                RetryDelegate onTimeout = default(RetryDelegate),
                string tag = default(string))
            where TDocument : class, ITableEntity
        {
            return CreateOrReplaceBatch(entities,
                row => getRowKey(row).AsRowKey(),
                row => row.RowKey.GeneratePartitionKey(),
                onSuccess,
                onFailure,
                onTimeout,
                tag);
        }

        public IEnumerableAsync<TResult> CreateOrReplaceBatch<TDocument, TResult>(IEnumerable<TDocument> entities,
                Func<TDocument, string> getRowKey,
                Func<TDocument, string> getPartitionKey,
                Func<TDocument, TResult> onSuccess,
                Func<TDocument, TResult> onFailure,
                RetryDelegate onTimeout = default(RetryDelegate),
                string tag = default(string))
            where TDocument : class, ITableEntity
        {
            return entities
                .Select(
                    row =>
                    {
                        row.RowKey = getRowKey(row);
                        row.PartitionKey = getPartitionKey(row);
                        return row;
                    })
                .GroupBy(row => row.PartitionKey)
                .Select(grp => CreateOrReplaceBatchAsync(grp.Key, grp.ToArray()))
                .AsyncEnumerable()
                .OnComplete(
                    (resultss) =>
                    {
                        if (tag.IsNullOrWhiteSpace())
                            return;

                        if (!resultss.Any())
                            Console.WriteLine($"Batch[{tag}]:saved 0 {typeof(TDocument).Name} documents across 0 partitions.");

                        Console.WriteLine($"Batch[{tag}]:saved {resultss.Sum(results => results.Length)} {typeof(TDocument).Name} documents across {resultss.Length} partitions.");
                    })
                .SelectMany(
                    trs =>
                    {
                        return trs
                            .Select(
                                tableResult =>
                                {
                                    var resultDocument = (tableResult.Result as TDocument);
                                    if (tableResult.HttpStatusCode < 400)
                                        return onSuccess(resultDocument);
                                    return onFailure(resultDocument);
                                });
                    });
        }

        public async Task<TableResult[]> CreateOrReplaceBatchAsync<TDocument>(string partitionKey, TDocument[] entities,
                RetryDelegate onTimeout = default(RetryDelegate))
            where TDocument : class, ITableEntity
        {
            if (!entities.Any())
                return new TableResult[] { };
            
            var table = GetTable<TDocument>();
            var bucketCount = (entities.Length / 100) + 1;
            var results = await entities
                .Split(x => entities.Length / bucketCount)
                .Select(
                    async entitySet =>
                    {
                        if(!entitySet.Any())
                            return new TableResult[] { };

                        var batch = new TableBatchOperation();

                        foreach (var row in entitySet)
                        {
                            batch.InsertOrReplace(row);
                        }

                        // submit
                        try
                        {
                            var resultList = await table.ExecuteBatchAsync(batch);
                            return resultList.ToArray();
                        } catch (StorageException storageException)
                        {
                            if (storageException.RequestInformation.ExtendedErrorInformation.ErrorCode == "InvalidDuplicateRow")
                                return new TableResult[] { };
                            return new TableResult[] { };
                        }
                    })
                .WhenAllAsync()
                .SelectManyAsync()
                .ToArrayAsync();

            return results;
        }

        public override async Task<TResult> UpdateIfNotModifiedAsync<TData, TResult>(TData data,
            Func<TResult> success,
            Func<TResult> documentModified,
            Func<ExtendedErrorInformationCodes, string, TResult> onFailure = null,
            RetryDelegate onTimeout = null)
        {
            try
            {
                var table = GetTable<TData>();
                var update = TableOperation.Replace(data);
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
                                    if (default(RetryDelegate) == onTimeout)
                                        onTimeout = GetRetryDelegate();
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

        public override async Task<TResult> DeleteAsync<TData, TResult>(TData document,
            Func<TResult> success,
            Func<TResult> onNotFound,
            Func<ExtendedErrorInformationCodes, string, TResult> onFailure =
                default(Func<ExtendedErrorInformationCodes, string, TResult>),
            RetryDelegate onTimeout = default(RetryDelegate))
        {
            var table = GetTable<TData>();
            if (default(CloudTable) == table)
                return onNotFound();

            if (string.IsNullOrEmpty(document.ETag))
                document.ETag = "*";

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
                                    if (default(RetryDelegate) == onTimeout)
                                        onTimeout = GetRetryDelegate();
                                    await onTimeout(se.RequestInformation.HttpStatusCode, se,
                                        async () =>
                                        {
                                            timeoutResult = await DeleteAsync(document, success, onNotFound, onFailure, onTimeout);
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

        public override async Task<TResult> CreateAsync<TResult, TDocument>(string rowKey, string partitionKey, TDocument document,
           Func<TResult> onSuccess,
           Func<TResult> onAlreadyExists,
            Func<ExtendedErrorInformationCodes, string, TResult> onFailure =
                default(Func<ExtendedErrorInformationCodes, string, TResult>),
           RetryDelegate onTimeout = default(RetryDelegate))
        {
            document.RowKey = rowKey;
            document.PartitionKey = partitionKey;
            while (true)
            {
                var table = GetTable<TDocument>();
                try
                {
                    TableResult tableResult = null;
                    var insert = TableOperation.Insert(document);
                    tableResult = await table.ExecuteAsync(insert);
                    return onSuccess();
                }
                catch (StorageException ex)
                {
                    if (ex.IsProblemTableDoesNotExist())
                    {
                        try
                        {
                            await table.CreateIfNotExistsAsync();
                        }
                        catch (StorageException createEx)
                        {
                            // Catch bug with azure storage table client library where
                            // if two resources attempt to create the table at the same
                            // time one gets a precondtion failed error.
                            System.Threading.Thread.Sleep(1000);
                            createEx.ToString();
                        }
                        continue;
                    }

                    if (ex.IsProblemResourceAlreadyExists())
                        return onAlreadyExists();

                    if (ex.IsProblemTimeout())
                    {
                        TResult result = default(TResult);
                        if (default(RetryDelegate) == onTimeout)
                            onTimeout = GetRetryDelegate();
                        await onTimeout(ex.RequestInformation.HttpStatusCode, ex,
                            async () =>
                            {
                                result = await CreateAsync(rowKey, partitionKey, document, onSuccess, onAlreadyExists, onFailure, onTimeout);
                            });
                        return result;
                    }

                    if (ex.InnerException is System.Net.WebException)
                    {
                        try
                        {
                            var innerException = ex.InnerException as System.Net.WebException;
                            var responseContentStream = innerException.Response.GetResponseStream();
                            var responseContentBytes = responseContentStream.ToBytes();
                            var responseString = responseContentBytes.ToText();
                            throw new Exception(responseString);
                        }
                        catch (Exception)
                        {
                        }
                        throw;
                    }
                    //if(ex.InnerException.Response)

                    throw;
                }
                catch (Exception general_ex)
                {
                    var message = general_ex;
                    throw;
                }

            }
        }

        public override async Task<TResult> FindByIdAsync<TEntity, TResult>(string rowKey, string partitionKey,
            Func<TEntity, TResult> onSuccess, Func<TResult> onNotFound,
            Func<ExtendedErrorInformationCodes, string, TResult> onFailure =
                default(Func<ExtendedErrorInformationCodes, string, TResult>),
            RetryDelegate onTimeout = default(RetryDelegate))
        {
            var table = GetTable<TEntity>();
            var operation = TableOperation.Retrieve<TEntity>(partitionKey, rowKey);
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
                    if (default(RetryDelegate) == onTimeout)
                        onTimeout = GetRetryDelegate();
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

        #endregion

        public async Task<TResult> CreateAsync<TResult, TDocument>(Guid id, TDocument document,
            Func<TResult> onSuccess,
            Func<TResult> onAlreadyExists,
            Func<ExtendedErrorInformationCodes, string, TResult> onFailure =
                default(Func<ExtendedErrorInformationCodes, string, TResult>),
            RetryDelegate onTimeout = default(RetryDelegate),
            Func<string, string> mutatePartition = default(Func<string, string>))
            where TDocument : class, ITableEntity
        {
            var rowKey = id.AsRowKey();
            var partitionKey = rowKey.GeneratePartitionKey();
            if (!mutatePartition.IsDefaultOrNull())
                partitionKey = mutatePartition(partitionKey);
            return await CreateAsync(rowKey, partitionKey, document, onSuccess, onAlreadyExists, onFailure, onTimeout);
        }

        public async Task<TResult> CreateAsync<TResult, TDocument>(Guid id, string partitionKey, TDocument document,
           Func<TResult> onSuccess,
           Func<TResult> onAlreadyExists,
                Func<ExtendedErrorInformationCodes, string, TResult> onFailure =
                    default(Func<ExtendedErrorInformationCodes, string, TResult>),
           RetryDelegate onTimeout = default(RetryDelegate))
           where TDocument : class, ITableEntity
        {
            var rowKey = id.AsRowKey();
            return await CreateAsync(rowKey, partitionKey, document, onSuccess, onAlreadyExists, onFailure, onTimeout);
        }

        public Task<TResult> CreateOrUpdateAsync<TDocument, TResult>(Guid id,
                Func<bool, TDocument, SaveDocumentDelegate<TDocument>, Task<TResult>> success,
                Func<ExtendedErrorInformationCodes, string, TResult> onFailure =
                    default(Func<ExtendedErrorInformationCodes, string, TResult>),
                RetryDelegate onTimeout = default(RetryDelegate),
                Func<string, string> mutatePartition = default(Func<string, string>))
            where TDocument : class, ITableEntity
        {
            var rowKey = id.AsRowKey();
            var partitionKey = rowKey.GeneratePartitionKey();
            if (!mutatePartition.IsDefaultOrNull())
                partitionKey = mutatePartition(partitionKey);
            return CreateOrUpdateAsync(rowKey, partitionKey, success, onFailure, onTimeout);
        }

        public Task<TResult> CreateOrUpdateAsync<TDocument, TResult>(Guid id, string partitionKey,
                Func<bool, TDocument, SaveDocumentDelegate<TDocument>, Task<TResult>> success,
                Func<ExtendedErrorInformationCodes, string, TResult> onFailure =
                    default(Func<ExtendedErrorInformationCodes, string, TResult>),
                RetryDelegate onTimeout = default(RetryDelegate))
            where TDocument : class, ITableEntity
        {
            return CreateOrUpdateAsync(id.AsRowKey(), partitionKey, success, onFailure, onTimeout);
        }

        public async Task<TResult> CreateOrUpdateAsync<TDocument, TResult>(string rowKey, string partitionKey,
                Func<bool, TDocument, SaveDocumentDelegate<TDocument>, Task<TResult>> success,
                Func<ExtendedErrorInformationCodes, string, TResult> onFailure =
                    default(Func<ExtendedErrorInformationCodes, string, TResult>),
                RetryDelegate onTimeout = default(RetryDelegate))
            where TDocument : class, ITableEntity
        {
            return await await FindByIdAsync(rowKey, partitionKey,
                async (TDocument document) =>
                {
                    var globalResult = default(TResult);
                    bool useGlobalResult = false;
                    var localResult = await success(false, document,
                        async (documentNew) =>
                        {
                            useGlobalResult = await await this.UpdateIfNotModifiedAsync(documentNew,
                                () => false.ToTask(),
                                async () =>
                                {
                                    globalResult = await this.CreateOrUpdateAsync(rowKey, partitionKey, success, onFailure, onTimeout);
                                    return true;
                                });
                        });
                    return useGlobalResult ? globalResult : localResult;
                },
                async () =>
                {
                    var document = Activator.CreateInstance<TDocument>();
                    document.RowKey = rowKey;
                    document.PartitionKey = partitionKey;
                    var globalResult = default(TResult);
                    bool useGlobalResult = false;
                    var localResult = await success(true, document,
                        async (documentNew) =>
                        {
                            useGlobalResult = await await this.CreateAsync(rowKey, partitionKey, documentNew,
                                () => false.ToTask(),
                                async () =>
                                {
                                    // TODO: backoff here
                                    globalResult = await this.CreateOrUpdateAsync(rowKey, partitionKey, success, onFailure, onTimeout);
                                    return true;
                                });
                        });
                    return useGlobalResult ? globalResult : localResult;
                });
        }

        public async Task<TResult> DeleteIfAsync<TDocument, TResult>(Guid documentId,
            Func<TDocument, Func<Task>, Task<TResult>> found,
            Func<TResult> notFound,
            Func<ExtendedErrorInformationCodes, string, TResult> onFailure =
                    default(Func<ExtendedErrorInformationCodes, string, TResult>),
            RetryDelegate onTimeout = default(RetryDelegate),
            Func<string, string> mutatePartition = default(Func<string, string>))
            where TDocument : class, ITableEntity
        {
            var rowKey = documentId.AsRowKey();
            var partitionKey = rowKey.GeneratePartitionKey();
            if (!mutatePartition.IsDefaultOrNull())
                partitionKey = mutatePartition(partitionKey);
            return await DeleteIfAsync(rowKey, partitionKey, found, notFound, onFailure, onTimeout);
        }

        public async Task<TResult> DeleteIfAsync<TDocument, TResult>(Guid documentId, string partitionKey,
            Func<TDocument, Func<Task>, Task<TResult>> found,
            Func<TResult> notFound,
                Func<ExtendedErrorInformationCodes, string, TResult> onFailure =
                    default(Func<ExtendedErrorInformationCodes, string, TResult>),
            RetryDelegate onTimeout = default(RetryDelegate))
            where TDocument : class, ITableEntity
        {
            var rowKey = documentId.AsRowKey();
            return await DeleteIfAsync(rowKey, partitionKey, found, notFound, onFailure, onTimeout);
        }

        public async Task<TResult> DeleteIfAsync<TDocument, TResult>(string rowKey, string partitionKey,
            Func<TDocument, Func<Task>, Task<TResult>> found,
            Func<TResult> notFound,
                Func<ExtendedErrorInformationCodes, string, TResult> onFailure =
                    default(Func<ExtendedErrorInformationCodes, string, TResult>),
            RetryDelegate onTimeout = default(RetryDelegate))
            where TDocument : class, ITableEntity
        {
            return await await this.FindByIdAsync<TDocument, Task<TResult>>(rowKey, partitionKey,
                async (data) =>
                {
                    bool useResultNotFound = false;
                    var resultNotFound = default(TResult);
                    var resultFound = await found(data,
                        async () =>
                        {
                            useResultNotFound = await DeleteAsync(data,
                                () => false,
                                () =>
                                {
                                    resultNotFound = notFound();
                                    return true;
                                });
                        });

                    return useResultNotFound ? resultNotFound : resultFound;
                },
                notFound.AsAsyncFunc(),
                onFailure.AsAsyncFunc(),
                onTimeout);
        }

        #region Find

        public async Task<TResult> FindByIdAsync<TEntity, TResult>(Guid documentId,
            Func<TEntity, TResult> onSuccess,
            Func<TResult> onNotFound,
            Func<ExtendedErrorInformationCodes, string, TResult> onFailure =
                default(Func<ExtendedErrorInformationCodes, string, TResult>),
            RetryDelegate onTimeout = default(RetryDelegate),
            Func<string, string> mutatePartition = default(Func<string, string>))
                   where TEntity : class, ITableEntity
        {
            var rowKey = documentId.AsRowKey();
            var partitionKey = rowKey.GeneratePartitionKey();
            if (!mutatePartition.IsDefaultOrNull())
                partitionKey = mutatePartition(partitionKey);
            return await FindByIdAsync(rowKey, partitionKey, onSuccess, onNotFound, onFailure, onTimeout);
        }

        public Task<TResult> FindByIdAsync<TEntity, TResult>(Guid documentId, string partitionKey,
            Func<TEntity, TResult> onSuccess,
            Func<TResult> onNotFound,
            Func<ExtendedErrorInformationCodes, string, TResult> onFailure =
                default(Func<ExtendedErrorInformationCodes, string, TResult>),
            RetryDelegate onTimeout = default(RetryDelegate))
                   where TEntity : class, ITableEntity
        {
            var rowKey = documentId.AsRowKey();
            return FindByIdAsync(rowKey, partitionKey, onSuccess, onNotFound, onFailure, onTimeout);
        }

        public async Task<TResult> TotalDocumentCountAsync<TData, TResult>(
            Func<long, TResult> success,
            Func<TResult> failure)
            where TData : class, ITableEntity, new()
        {
            var query = new TableQuery<TData>();
            var table = GetTable<TData>();

            // Reduce amount of data returned with projection query since we only want the count
            // TODO - I'm not sure that this is reducing our data quantity returned
            var projectionQuery = new TableQuery<TData>().Select(new[] { "PartitionKey" });

            try
            {
                TableContinuationToken token = null;
                long totalCount = 0;
                do
                {
                    var segment = await table.ExecuteQuerySegmentedAsync(projectionQuery, token);
                    token = segment.ContinuationToken;
                    totalCount += segment.Results.Count;
                } while (token != null);
                return success(totalCount);
            }
            catch (StorageException se)
            {
                if (se.IsProblemDoesNotExist() || se.IsProblemTableDoesNotExist())
                    return failure();
            }
            return failure();
        }

        private async Task<TResult> FindAllRecursiveAsync<TData, TResult>(CloudTable table, TableQuery<TData> query,
            TData[] oldData, TableContinuationToken token,
            Func<TData[], bool, Func<Task<TResult>>, TResult> onFound)
            where TData : class, ITableEntity, new()
        {
            try
            {
                var segment = await table.ExecuteQuerySegmentedAsync(query, token);
                var newToken = segment.ContinuationToken;
                var newData = oldData.Concat(segment).ToArray();
                return onFound(
                    newData,
                    newToken != default(TableContinuationToken),
                    () => FindAllRecursiveAsync(table, query, newData, newToken, onFound));
            } catch (StorageException se)
            {
                if (se.IsProblemDoesNotExist() || se.IsProblemTableDoesNotExist())
                    return onFound(
                        oldData,
                        false,
                        () => FindAllRecursiveAsync(table, query, oldData, default(TableContinuationToken), onFound));
                throw;
            }
        }

        public async Task<TResult> FindAllAsync<TData, TResult>(
            Func<TData[], bool, Func<Task<TResult>>, TResult> onFound)
            where TData : class, ITableEntity, new()
        {
            var query = new TableQuery<TData>();
            var table = GetTable<TData>();
            return await FindAllRecursiveAsync(table, query, new TData[] { }, default(TableContinuationToken), onFound);
        }

        public async Task<TResult> FindAllAsync<TData, TResult>(
            Func<TData[], TResult> onFound)
            where TData : class, ITableEntity, new()
        {
            return await await FindAllAsync<TData, Task<TResult>>(
                async (data, continuable, fetchAsync) =>
                {
                    if (continuable)
                        return await await fetchAsync();
                    return onFound(data);
                });
        }

        public async Task<IEnumerable<TData>> FindAllByQueryAsync<TData>(TableQuery<TData> tableQuery)
            where TData : class, ITableEntity, new()
        {
            var table = GetTable<TData>();
            try
            {
                IEnumerable<List<TData>> lists = new List<TData>[] { };
                TableContinuationToken token = null;
                do
                {
                    var segment = await table.ExecuteQuerySegmentedAsync(tableQuery, token);
                    token = segment.ContinuationToken;
                    lists = lists.Append(segment.Results);
                } while (token != null);
                return lists.SelectMany();
            }
            catch (StorageException se)
            {
                if (se.IsProblemDoesNotExist() || se.IsProblemTableDoesNotExist())
                    return new TData[] { };
                throw;
            };
        }

        public IEnumerableAsync<TData> FindAllByPartition<TData>(string partitionKeyValue)
            where TData : class, ITableEntity, new()
        {
            string filter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKeyValue);

            var tableQuery = new TableQuery<TData>().Where(filter);
            return FindAllAsync(tableQuery);
        }

        public async Task<IEnumerable<TData>> FindAllByPartitionAsync<TData>(string partitionKeyValue)
            where TData : class, ITableEntity, new()
        {
            string filter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKeyValue);

            var tableQuery =
                   new TableQuery<TData>().Where(filter);

            //Execute the query
            var table = GetTable<TData>();
            try
            {
                IEnumerable<List<TData>> lists = new List<TData>[] { };
                TableContinuationToken token = null;
                do
                {
                    var segment = await table.ExecuteQuerySegmentedAsync(tableQuery, token);
                    token = segment.ContinuationToken;
                    lists = lists.Append(segment.Results);
                } while (token != null);
                return lists.SelectMany();
            }
            catch (StorageException se)
            {
                if (se.IsProblemDoesNotExist() || se.IsProblemTableDoesNotExist())
                    return new TData[] { };
                throw;
            };
        }

        #endregion

        #region Locking

        public delegate Task<TResult> WhileLockedDelegateAsync<TDocument, TResult>(TDocument document,
            Func<UpdateDelegate<TDocument, Task>, Task> unlockAndSave,
            Func<Task> unlock);

        public delegate Task<TResult> ConditionForLockingDelegateAsync<TDocument, TResult>(TDocument document,
            Func<Task<TResult>> continueLocking);
        public delegate Task<TResult> ContinueAquiringLockDelegateAsync<TDocument, TResult>(int retryAttempts, TimeSpan elapsedTime,
                TDocument document,
            Func<Task<TResult>> continueAquiring,
            Func<Task<TResult>> force = default(Func<Task<TResult>>));

        public Task<TResult> LockedUpdateAsync<TDocument, TResult>(Guid id,
                Expression<Func<TDocument, bool>> lockedPropertyExpression,
            WhileLockedDelegateAsync<TDocument, TResult> onLockAquired,
            Func<TResult> onNotFound,
            Func<TResult> onLockRejected = default(Func<TResult>),
            ContinueAquiringLockDelegateAsync<TDocument, TResult> onAlreadyLocked =
                    default(ContinueAquiringLockDelegateAsync<TDocument, TResult>),
                ConditionForLockingDelegateAsync<TDocument, TResult> shouldLock =
                    default(ConditionForLockingDelegateAsync<TDocument, TResult>),
                RetryDelegateAsync<Task<TResult>> onTimeout = default(RetryDelegateAsync<Task<TResult>>),
            Func<string, string> mutatePartition = default(Func<string, string>))
            where TDocument : TableEntity => LockedUpdateAsync(id, lockedPropertyExpression, 0, DateTime.UtcNow,
                onLockAquired,
                onNotFound,
                onLockRejected,
                onAlreadyLocked,
                shouldLock,
                onTimeout,
                mutatePartition);

        private async Task<TResult> LockedUpdateAsync<TDocument, TResult>(Guid id,
                Expression<Func<TDocument, bool>> lockedPropertyExpression,
                int retryCount,
                DateTime initialPass,
            WhileLockedDelegateAsync<TDocument, TResult> onLockAquired,
            Func<TResult> onNotFound,
            Func<TResult> onLockRejected = default(Func<TResult>),
                ContinueAquiringLockDelegateAsync<TDocument, TResult> onAlreadyLocked = 
                    default(ContinueAquiringLockDelegateAsync<TDocument, TResult>),
                ConditionForLockingDelegateAsync<TDocument, TResult> shouldLock =
                    default(ConditionForLockingDelegateAsync<TDocument, TResult>),
                RetryDelegateAsync<Task<TResult>> onTimeout = default(RetryDelegateAsync<Task<TResult>>),
                Func<string, string> mutatePartition = default(Func<string, string>))
            where TDocument : TableEntity
        {
            if (default(RetryDelegateAsync<Task<TResult>>) == onTimeout)
                onTimeout = GetRetryDelegateContentionAsync<Task<TResult>>();
            
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
                var documentLocked = (bool)(fieldInfo != null ? fieldInfo.GetValue(document) : propertyInfo.GetValue(document));
                return documentLocked;
            }
            void lockDocument(TDocument document)
            {
                if (fieldInfo != null)
                    fieldInfo.SetValue(document, true);
                else
                    propertyInfo.SetValue(document, true);
            }
            void unlockDocument(TDocument documentLocked)
            {
                documentLocked.SetFieldOrProperty(false, fieldInfo, propertyInfo);
            }

            // retryIncrease because some retries don't count
            Task<TResult> retry(int retryIncrease) => LockedUpdateAsync(id,
                    lockedPropertyExpression, retryCount + retryIncrease, initialPass,
                onLockAquired, 
                onNotFound, 
                onLockRejected,
                onAlreadyLocked, 
                    shouldLock,
                    onTimeout,
                    mutatePartition);

            #endregion

            return await await this.FindByIdAsync(id,
                async (TDocument document) =>
                {
                    async Task<TResult> execute()
                    {
                        // Save document in locked state
                        return await await this.UpdateIfNotModifiedAsync(document,
                            () => PerformLockedCallback(id, document, unlockDocument, onLockAquired, mutatePartition),
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
                onNotFound.AsAsyncFunc(),
                mutatePartition:mutatePartition);
                // TODO: onTimeout:onTimeout);
        }

        private async Task<TResult> PerformLockedCallback<TDocument, TResult>(
            Guid id,
            TDocument documentLocked,
            Action<TDocument> unlockDocument,
            WhileLockedDelegateAsync<TDocument, TResult> success,
            Func<string, string> mutatePartition)
            where TDocument : TableEntity
        {
            try
            {
                var result =  await success(documentLocked,
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
                            () => false,
                            mutatePartition:mutatePartition);
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
                            () => false,
                            mutatePartition: mutatePartition);
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
                    () => false,
                    mutatePartition: mutatePartition);
                throw;
            }
        }

        #endregion
    }
}
