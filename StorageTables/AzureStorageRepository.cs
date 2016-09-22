using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using BlackBarLabs.Persistence.Azure.StorageTables.RelationshipDocuments;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Microsoft.WindowsAzure.Storage.Table;
using BlackBarLabs.Collections.Async;
using BlackBarLabs.Core.Extensions;

namespace BlackBarLabs.Persistence.Azure.StorageTables
{
    public partial class AzureStorageRepository
    {
        public readonly CloudTableClient TableClient;
        private const int retryHttpStatus = 200;

        #region Utility methods

        private static RetryDelegate GetRetryDelegate()
        {
            var retriesAttempted = 0;
            var retryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(4), 10);
            return async(statusCode, ex, retry) =>
            {
                TimeSpan retryDelay;
                bool shouldRetry = retryPolicy.ShouldRetry(retriesAttempted++, statusCode, ex, out retryDelay, null);
                if (!shouldRetry)
                    throw new Exception("After " + retriesAttempted + "attempts finding the resource timed out");
                await Task.Delay(retryDelay);
                await retry();
            };
        }

        private static RetryDelegateAsync<TResult> GetRetryDelegateContentionAsync<TResult>(
            int maxRetries = 10)
        {
            var retriesAttempted = 0;
            var lastFail = default(long);
            var rand = new Random();
            return 
                async (retry, timeout) =>
                {
                    bool shouldRetry = retriesAttempted <= maxRetries;
                    if (!shouldRetry)
                        return timeout(retriesAttempted);
                    var failspan = (retriesAttempted > 0) ?
                        DateTime.UtcNow.Ticks - lastFail :
                        0;
                    lastFail = DateTime.UtcNow.Ticks;

                    retriesAttempted++;
                    var bobble = rand.NextDouble() * 2.0;
                    var retryDelay = TimeSpan.FromTicks((long)(failspan * bobble));
                    await Task.Delay(retryDelay);
                    return retry();
                };
        }

        private static RetryDelegateAsync<TResult> GetRetryDelegateCollisionAsync<TResult>(
            TimeSpan delay = default(TimeSpan),
            TimeSpan limit = default(TimeSpan),
            int maxRetries = 10)
        {
            if (default(TimeSpan) == delay)
                delay = TimeSpan.FromSeconds(0.5);

            if (default(TimeSpan) == delay)
                limit = TimeSpan.FromSeconds(60.0);

            var retriesAttempted = 0;
            var rand = new Random();
            long delayFactor = 1;
            return
                async (retry, timeout) =>
                {
                    bool shouldRetry = retriesAttempted <= maxRetries;
                    if (!shouldRetry)
                        return timeout(retriesAttempted);
                    retriesAttempted++;
                    var bobble = rand.NextDouble() * 2.0;
                    var delayMultiplier = ((double)(delayFactor >> 1)) + ((double)delayFactor * bobble);
                    var retryDelay = TimeSpan.FromTicks((long)(delay.Ticks * delayMultiplier));
                    delayFactor = delayFactor << 1;
                    delay = TimeSpan.FromSeconds(delay.TotalSeconds + (retriesAttempted * delay.TotalSeconds));
                    await Task.Delay(retryDelay);
                    return retry();
                };
        }

        private CloudTable GetTable<T>()
        {
            var tableName = typeof(T).Name.ToLower();
            return TableClient.GetTableReference(tableName);
        }

        private static TResult RepeatAtomic<TResult>(Func<TResult> callback, Func<TResult> doOver)
        {
            try
            {
                return callback();
            }
            catch (StorageException ex)
            {
                if (!ex.IsProblemPreconditionFailed() &&
                    !ex.IsProblemTimeout())
                { throw; }

                return doOver();
            }
        }

        #endregion

        #region Generic delegates

        public delegate Task SaveDocumentDelegate<TDocument>(TDocument documentInSavedState);
        public delegate TResult NotFoundDelegate<TResult>();
        public delegate TResult AlreadyExitsDelegate<TResult>();
        public delegate Task RetryDelegate(int statusCode, Exception ex, Func<Task> retry);
        public delegate Task<TResult> RetryDelegateAsync<TResult>(
            Func<TResult> retry,
            Func<int, TResult> timeout);

        #endregion

        #region Direct methods

        [Obsolete("UpdateIfNotModifiedAsync<TData> is deprecated, please use UpdateIfNotModifiedAsync<TData, TResult> instead.")]
        public async Task<TData> UpdateIfNotModifiedAsync<TData>(TData data) where TData : ITableEntity
        {
            var table = GetTable<TData>();
            var update = TableOperation.Replace(data);
            await table.ExecuteAsync(update);
            return data;
        }

        public async Task<TResult> UpdateIfNotModifiedAsync<TData, TResult>(TData data,
            Func<TResult> success,
            Func<TResult> documentModified,
            RetryDelegate onTimeout = default(RetryDelegate))
            where TData : ITableEntity
        {
            if (default(RetryDelegate) == onTimeout)
                onTimeout = GetRetryDelegate();

            try
            {
                var table = GetTable<TData>();
                var update = TableOperation.Replace(data);
                await table.ExecuteAsync(update);
                return success();
            }
            catch (StorageException ex)
            {
                if (ex.IsProblemTimeout())
                {
                    var timeoutResult = default(TResult);
                    await onTimeout(ex.RequestInformation.HttpStatusCode, ex,
                        async () =>
                        {
                            timeoutResult = await UpdateIfNotModifiedAsync(data, success, documentModified, onTimeout);
                        });
                    return timeoutResult;
                }
                if (ex.IsProblemPreconditionFailed())
                    return documentModified();
                throw;
            }
        }
        
        #endregion

        public async Task<TResult> CreateOrUpdateAtomicAsync<TResult, TData>(Guid id,
            Func<TData, SaveDocumentDelegate<TData>, Task<TResult>> atomicModifier,
            RetryDelegate onTimeout = default(RetryDelegate))
            where TData : class, ITableEntity
        {
            if (default(RetryDelegate) == onTimeout)
                onTimeout = GetRetryDelegate();

            var result = await await FindByIdAsync(id,
                async (TData document) =>
                {
                    bool failOverride = false;
                    TResult failResult = default(TResult);
                    var resultSuccess = await atomicModifier(document,
                        async (updateDocumentWith) =>
                        {
                            failOverride = await await UpdateIfNotModifiedAsync(updateDocumentWith,
                                async () => await Task.FromResult(true),
                                async () =>
                                {
                                    failResult = await CreateOrUpdateAtomicAsync(id, atomicModifier, onTimeout);
                                    return false;
                                });
                        });
                    return (!failOverride) ?
                        resultSuccess :
                        failResult;
                },
                async () =>
                {
                    bool useRecursiveResult = false;
                    var recursiveResult = default(TResult);
                    var r = await atomicModifier(
                        default(TData),
                        async (createDocumentWith) =>
                        {
                            useRecursiveResult = await await CreateAsync<Task<bool>, TData>(id, createDocumentWith,
                                () => Task.FromResult(false),
                                async () =>
                                {
                                    recursiveResult = await CreateOrUpdateAtomicAsync(id, atomicModifier, onTimeout);
                                    return true;
                                },
                                onTimeout);
                        });
                    if(useRecursiveResult)
                        return recursiveResult;
                    return r;
                },
                onTimeout);
            return result;
        }
        
        public delegate TResult CreateSuccessDelegate<TResult>();
        public async Task<TResult> CreateAsync<TResult, TDocument>(Guid id, TDocument document,
            CreateSuccessDelegate<TResult> onSuccess,
            AlreadyExitsDelegate<TResult> onAlreadyExists,
            RetryDelegate onTimeout = default(RetryDelegate))
            where TDocument : class, ITableEntity
        {
            if (default(RetryDelegate) == onTimeout)
                onTimeout = GetRetryDelegate();

            document.SetId(id);
            
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
                        await table.CreateIfNotExistsAsync();
                        continue;
                    }

                    if (ex.IsProblemResourceAlreadyExists())
                        return onAlreadyExists();

                    if (ex.IsProblemTimeout())
                    {
                        TResult result = default(TResult);
                        await onTimeout(ex.RequestInformation.HttpStatusCode, ex,
                            async () =>
                            {
                                result = await CreateAsync(id, document, onSuccess, onAlreadyExists, onTimeout);
                            });
                        return result;
                    }

                    throw;
                }
                catch (Exception general_ex)
                {
                    var message = general_ex;
                    throw;
                }

            }
        }
        
        public async Task<TResult> CreateOrUpdateAsync<TDocument, TResult>(Guid id,
                Func<bool, TDocument, SaveDocumentDelegate<TDocument>, Task<TResult>> success,
                RetryDelegate onTimeout = default(RetryDelegate))
            where TDocument : class, ITableEntity
        {
            Func<TDocument, Task<TResult>> performCallback =
                async (document) =>
                {
                    var globalResult = default(TResult);
                    bool useGlobalResult = false;
                    var localResult = await success(true, document,
                        async (documentNew) =>
                        {
                            useGlobalResult = await await this.UpdateIfNotModifiedAsync(documentNew,
                                () => false.ToTask(),
                                async () =>
                                {
                                    globalResult = await this.CreateOrUpdateAsync(id, success, onTimeout);
                                    return true;
                                });
                        });
                    return useGlobalResult ? globalResult : localResult;
                };

            var result = await await FindByIdAsync(id,
                (TDocument document) => performCallback(document),
                () =>
                {
                    var document = Activator.CreateInstance<TDocument>();
                    document.SetId(id);
                    return performCallback(document);
                });
            return result;
        }

        public async Task<bool> DeleteAsync<TData>(TData data, Func<TData, TData, bool> deleteIssueCallback, int numberOfTimesToRetry = int.MaxValue)
            where TData : class, ITableEntity
        {
            while (true)
            {
                try
                {
                    var table = GetTable<TData>();
                    if (string.IsNullOrEmpty(data.ETag)) data.ETag = "*";
                    var delete = TableOperation.Delete(data);
                    await table.ExecuteAsync(delete);
                    return true;
                }
                catch (StorageException ex)
                {
                    if (ex.IsProblemPreconditionFailed())
                    {
                        var mostRecentData = await FindById<TData>(data.RowKey);
                        var deleteMostRecent = deleteIssueCallback.Invoke(data, mostRecentData);
                        if (!deleteMostRecent)
                            return false;
                        data = mostRecentData;
                        continue;
                    }
                }
                numberOfTimesToRetry--;
                if (numberOfTimesToRetry <= 0)
                    throw new Exception("Tries exceeded");
            }
        }

        public delegate TResult DeleteDelegate<TResult, TData>(TData storedDocument, Action delete);
        public async Task<TResult> DeleteAsync<TResult, TData>(Guid documentId,
            DeleteDelegate<TResult, TData> success,
            NotFoundDelegate<TResult> onNotFound,
            RetryDelegate onTimeout = default(RetryDelegate))
            where TData : class, ITableEntity
        {
            if (default(RetryDelegate) == onTimeout)
                onTimeout = GetRetryDelegate();
            
            return await await this.FindByIdAsync<TData, Task<TResult>>(documentId,
                async (data) =>
                {
                    var table = GetTable<TData>();
                    if (default(CloudTable) == table)
                        return onNotFound();

                    bool needTodelete = false;
                    var result = success(data, () => needTodelete = true);

                    if(needTodelete)
                    {
                        return await DeleteAsync(data,
                            () => result,
                            () => onNotFound(),
                            onTimeout);
                    }
                    return result;
                },
                () => Task.FromResult(onNotFound()));
        }

        public async Task<TResult> DeleteAsync<TResult, TData>(TData document,
            Func<TResult> success,
            NotFoundDelegate<TResult> onNotFound,
            RetryDelegate onTimeout = default(RetryDelegate))
            where TData : class, ITableEntity
        {
            if (default(RetryDelegate) == onTimeout)
                onTimeout = GetRetryDelegate();

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
                if (se.IsProblemTableDoesNotExist() ||
                    se.IsProblemDoesNotExist())
                    return onNotFound();
                
                if (se.IsProblemTimeout())
                {
                    TResult timeoutResult = default(TResult);
                    await onTimeout(se.RequestInformation.HttpStatusCode, se,
                        async () =>
                        {
                            timeoutResult = await DeleteAsync(document, success, onNotFound, onTimeout);
                        });
                    return timeoutResult;
                }
                throw se;
            }
        }

        #region Find

        public delegate TResult FindByIdSuccessDelegate<TEntity, TResult>(TEntity document);
        public async Task<TResult> FindByIdAsync<TEntity, TResult>(Guid documentId,
            FindByIdSuccessDelegate<TEntity, TResult> onSuccess, Func<TResult> onNotFound,
            RetryDelegate onTimeout = default(RetryDelegate))
                   where TEntity : class, ITableEntity
        {
            if (default(RetryDelegate) == onTimeout)
                onTimeout = GetRetryDelegate();

            var rowKey = documentId.AsRowKey();
            var table = GetTable<TEntity>();
            var operation = TableOperation.Retrieve<TEntity>(rowKey.GeneratePartitionKey(), rowKey);
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
                    await onTimeout(se.RequestInformation.HttpStatusCode, se,
                        async () =>
                        {
                            result = await FindByIdAsync(documentId, onSuccess, onNotFound, onTimeout);
                        });
                    return result;
                }
                throw se;
            }
        }

        public IEnumerableAsync<Func<TData, Task>> FindAllAsync<TData>()
            where TData : class, ITableEntity, new()
        {
            var query = new TableQuery<TData>();
            var table = GetTable<TData>();
            return EnumerableAsync.YieldAsync<Func<TData, Task>>(
                async (yieldAsync) =>
                {
                    try
                    {
                        TableContinuationToken token = null;
                        do
                        {
                            var segment = await table.ExecuteQuerySegmentedAsync(query, token);
                            token = segment.ContinuationToken;
                            foreach (var result in segment.Results)
                                await yieldAsync(result);
                        } while (token != null);
                    }
                    catch (StorageException se)
                    {
                        if (se.IsProblemDoesNotExist() || se.IsProblemTableDoesNotExist())
                            return;
                        throw;
                    }
                });
        }

        #endregion

        public delegate Task<TResult> WhileLockedDelegateAsync<TDocument, TResult>(TDocument document,
            Func<UpdateDelegate<TDocument, Task>, Task> unlockAndSave,
            Func<Task> unlock);
        public async Task<TResult> LockedUpdateAsync<TDocument, TResult>(Guid id,
                Expression<Func<TDocument, bool>> lockedPropertyExpression,
                WhileLockedDelegateAsync<TDocument, TResult> success,
                Func<TResult> notFound,
                Func<TResult> lockingTimeout,
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
                Func<TResult> lockingTimedout = default(Func<TResult>),
                RetryDelegateAsync<Task<TResult>> onTimeout = default(RetryDelegateAsync<Task<TResult>>))
            where TDocument : TableEntity
        {
            if (default(Func<TResult>) == lockingTimedout)
                lockingTimedout = () => default(TResult);

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
                            async () => await LockedUpdateAsync(id, lockedPropertyExpression, shouldLock, success, lockingRejected, notFound, lockingTimedout, onTimeout),
                            (numberOfRetries) => lockingTimedout().ToTask());
                    }
                    lockDocument(document);

                    #endregion

                    // Save document in locked state
                    var resultFromFind = await await this.UpdateIfNotModifiedAsync(document,
                        async () => await PerformLockedCallback(id, document, unlockDocument, success),
                        () => this.LockedUpdateAsync(id, lockedPropertyExpression, shouldLock, success, lockingRejected, notFound, lockingTimedout, onTimeout));
                    return resultFromFind;
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
    }
}
