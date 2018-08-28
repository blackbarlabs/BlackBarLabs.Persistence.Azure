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
using BlackBarLabs.Collections.Async;
using BlackBarLabs.Extensions;
using BlackBarLabs.Linq;
using EastFive;
using EastFive.Linq;
using EastFive.Extensions;
using EastFive.Azure.StorageTables.Driver;
using EastFive.Linq.Async;
using BlackBarLabs.Linq.Async;

namespace BlackBarLabs.Persistence.Azure.StorageTables
{
    public partial class AzureStorageRepository : EastFive.Azure.StorageTables.Driver.AzureStorageDriver
    {
        public readonly CloudTableClient TableClient;
        private const int retryHttpStatus = 200;

        private readonly Exception retryException = new Exception();
        
        public AzureStorageRepository(CloudStorageAccount storageAccount)
        {
            TableClient = storageAccount.CreateCloudTableClient();
            TableClient.DefaultRequestOptions.RetryPolicy = retryPolicy;
        }

        public static AzureStorageRepository CreateRepository(
            string storageSettingConfigurationKeyName)
        {
            var storageSetting = Microsoft.Azure.CloudConfigurationManager.GetSetting(storageSettingConfigurationKeyName);
            var cloudStorageAccount = CloudStorageAccount.Parse(storageSetting);
            var azureStorageRepository = new AzureStorageRepository(cloudStorageAccount);
            return azureStorageRepository;
        }

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

        public async Task<TableResult[]> CreateOrReplaceBatchAsync<TDocument>(string partitionKey, TDocument[] entities,
                RetryDelegate onTimeout = default(RetryDelegate))
            where TDocument : class, ITableEntity
        {
            if (entities.Length == 0)
                return new TableResult[] { };

            var table = GetTable<TDocument>();
            var bucketCount = (entities.Length / 100) + 1;
            var results = await entities
                .Split(x => entities.Length / bucketCount)
                .Select(
                    async entitySet =>
                    {
                        var batch = new TableBatchOperation();

                        foreach (var row in entitySet)
                        {
                            batch.InsertOrReplace(row);
                        }

                        // submit
                        var resultList = await table.ExecuteBatchAsync(batch);
                        return resultList.ToArray();
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
                                    if(se.IsProblemDoesNotExist())
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
            RetryDelegate onTimeout = default(RetryDelegate))
            where TDocument : class, ITableEntity
        {
            var rowKey = id.AsRowKey();
            var partitionKey = rowKey.GeneratePartitionKey();
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
                RetryDelegate onTimeout = default(RetryDelegate))
            where TDocument : class, ITableEntity
        {
            var rowKey = id.AsRowKey();
            var partitionKey= rowKey.GeneratePartitionKey();
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
            RetryDelegate onTimeout = default(RetryDelegate))
            where TDocument : class, ITableEntity
        {
            var rowKey = documentId.AsRowKey();
            var partitionKey = rowKey.GeneratePartitionKey();
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
            RetryDelegate onTimeout = default(RetryDelegate))
                   where TEntity : class, ITableEntity
        {
            var rowKey = documentId.AsRowKey();
            var partitionKey = rowKey.GeneratePartitionKey();
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
        public async Task<TResult> LockedUpdateAsync<TDocument, TResult>(Guid id,
                Expression<Func<TDocument, bool>> lockedPropertyExpression,
                WhileLockedDelegateAsync<TDocument, TResult> success,
                Func<TResult> notFound,
                Func<Func<Task<TResult>>, Task<TResult>> lockingTimeout,
                RetryDelegateAsync<Task<TResult>> onTimeout = default(RetryDelegateAsync<Task<TResult>>))
            where TDocument : TableEntity
        {
            ConditionForLockingDelegateAsync<TDocument> shouldLock = (doc) => true.ToTask();
            Func<TResult> lockingRejected = () => default(TResult); // Not Possible!!!
            return await LockedUpdateAsync(id,
                lockedPropertyExpression, shouldLock,
                success, lockingRejected, notFound, lockingTimeout, onTimeout);
        }
        
        public delegate Task<bool> ConditionForLockingDelegateAsync<TDocument>(TDocument document);
        public async Task<TResult> LockedUpdateAsync<TDocument, TResult>(Guid id,
                Expression<Func<TDocument, bool>> lockedPropertyExpression,
                ConditionForLockingDelegateAsync<TDocument> shouldLock,
                WhileLockedDelegateAsync<TDocument, TResult> success,
                Func<TResult> lockingRejected,
                Func<TResult> notFound,
                Func<Func<Task<TResult>>, Task<TResult>> lockingTimedout = default(Func<Func<Task<TResult>>, Task<TResult>>),
                RetryDelegateAsync<Task<TResult>> onTimeout = default(RetryDelegateAsync<Task<TResult>>))
            where TDocument : TableEntity
        {
            if (default(Func<Func<Task<TResult>>, Task<TResult>>) == lockingTimedout)
                lockingTimedout = (force) => default(TResult).ToTask();

            if (default(RetryDelegateAsync<Task<TResult>>) == onTimeout)
                onTimeout = GetRetryDelegateContentionAsync<Task<TResult>>();

            #region lock property expressions for easy use later

            var lockedPropertyMember = ((MemberExpression)lockedPropertyExpression.Body).Member;
            var fieldInfo = lockedPropertyMember as FieldInfo;
            var propertyInfo = lockedPropertyMember as PropertyInfo;
            Func<TDocument, bool> isDocumentLocked =
                (document) =>
                {
                    var documentLocked = (bool)(fieldInfo != null ? fieldInfo.GetValue(document) : propertyInfo.GetValue(document));
                    return documentLocked;
                };
            Action<TDocument> lockDocument =
                (document) =>
                {
                    if (fieldInfo != null)
                        fieldInfo.SetValue(document, true);
                    else
                        propertyInfo.SetValue(document, true);
                };
            Action<TDocument> unlockDocument =
                (documentLocked) =>
                {
                    documentLocked.SetFieldOrProperty(false, fieldInfo, propertyInfo);
                };

            #endregion
            
            var result = await await this.FindByIdAsync(id,
                async (TDocument document) =>
                {
                    if (! await shouldLock(document))
                        return lockingRejected();

                    #region Set document to locked state if not already locked

                    var documentLocked = isDocumentLocked(document);
                    if (documentLocked)
                    {
                        return await await onTimeout(
                            () => LockedUpdateAsync(id, lockedPropertyExpression, shouldLock, success, lockingRejected, notFound, lockingTimedout, onTimeout),
                            (numberOfRetries) => lockingTimedout(
                                async () => await await this.UpdateIfNotModifiedAsync(document,
                                    () => PerformLockedCallback(id, document, unlockDocument, success),
                                    () => this.LockedUpdateAsync(id, lockedPropertyExpression, shouldLock, success, lockingRejected, notFound, lockingTimedout, onTimeout))));
                    }
                    lockDocument(document);

                    #endregion

                    // Save document in locked state
                    return await await this.UpdateIfNotModifiedAsync(document,
                        () => PerformLockedCallback(id, document, unlockDocument, success),
                        () => this.LockedUpdateAsync(id, lockedPropertyExpression, shouldLock, success, lockingRejected, notFound, lockingTimedout, onTimeout));
                },
                () => notFound().ToTask());
            return result;
        }

        private async Task<TResult> PerformLockedCallback<TDocument, TResult>(
            Guid id,
            TDocument documentLocked,
            Action<TDocument> unlockDocument,
            WhileLockedDelegateAsync<TDocument, TResult> success)
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

        private async Task Unlock<TDocument>(Guid id,
            Action<TDocument> mutateEntityToSaveAction)
            where TDocument : TableEntity
        {
            var unlockSucceeded = await UpdateAsync<TDocument, bool>(id,
                async (entityLocked, save) =>
                {
                    mutateEntityToSaveAction(entityLocked);
                    await save(entityLocked);
                    return true;
                },
                () => false);
        }

        #endregion
    }
}