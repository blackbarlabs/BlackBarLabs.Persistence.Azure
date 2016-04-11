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

        private static RetryDelegateAsync<TResult> GetRetryDelegateAsync<TResult>()
        {
            var retriesAttempted = 0;
            var retryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(4), 10);
            return async (statusCode, ex, retry, timeout) =>
            {
                TimeSpan retryDelay;
                bool shouldRetry = retryPolicy.ShouldRetry(retriesAttempted++, statusCode, ex, out retryDelay, null);
                if (!shouldRetry)
                    return timeout();
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
        public delegate Task<TResult> RetryDelegateAsync<TResult>(int statusCode, Exception ex,
            Func<TResult> retry,
            Func<TResult> timeout);

        #endregion

        #region Direct methods

        public async Task<TData> UpdateIfNotModifiedAsync<TData>(TData data) where TData : ITableEntity
        {
            var table = GetTable<TData>();
            var update = TableOperation.Merge(data);
            await table.ExecuteAsync(update);
            return data;
        }

        #endregion

        public delegate TResult UpdateDelegate<TData, TResult>(TData currentStorage, SaveDocumentDelegate<TData> saveNew);
        public async Task<TResult> UpdateAsync<TData, TResult>(Guid id,
            UpdateDelegate<TData, Task<TResult>> onUpdate,
            NotFoundDelegate<TResult> onNotFound,
            RetryDelegate onTimeout = default(RetryDelegate))
            where TData : class, ITableEntity
        {
            if (default(RetryDelegate) == onTimeout)
                onTimeout = GetRetryDelegate();

            return await await FindByIdAsync(id,
                async (TData currentStorage) =>
                {
                    try
                    {
                        var result = await onUpdate.Invoke(currentStorage, async (documentToSave) =>
                        {
                            await UpdateIfNotModifiedAsync(documentToSave);
                        });
                        return result;
                    }
                    catch (StorageException ex)
                    {
                        if (ex.IsProblemTimeout())
                        {
                            var timeoutResult = default(TResult);
                            await onTimeout(ex.RequestInformation.HttpStatusCode, ex,
                                async () =>
                                {
                                    timeoutResult = await UpdateAsync(id, onUpdate, onNotFound, onTimeout);
                                });
                            return timeoutResult;
                        }
                        if (ex.IsProblemPreconditionFailed())
                            return await UpdateAsync(id, onUpdate, onNotFound, onTimeout);
                        throw;
                    }
                },
                () => Task.FromResult(onNotFound()),
                onTimeout);
        }

        public delegate Task<TResult> SaveDocumentDelegateAsync<TDocument, TResult>(TDocument documentToSave,
            Func<TResult> success, Func<TResult> modified);
        public delegate Task<TResult> UpdateDelegateAsync<TDocument, TResult>(TDocument currentStorage,
            SaveDocumentDelegateAsync<TDocument, TResult> saveNew);
        public async Task<TResult> UpdateAsync<TData, TResult>(Guid id,
            UpdateDelegateAsync<TData, TResult> onUpdate,
            NotFoundDelegate<TResult> onNotFound,
            RetryDelegateAsync<Task<TResult>> onTimeout = default(RetryDelegateAsync<Task<TResult>>))
            where TData : class, ITableEntity
        {
            if (default(RetryDelegateAsync<Task<TResult>>) == onTimeout)
                onTimeout = GetRetryDelegateAsync<Task<TResult>>();

            return await await FindByIdAsync(id,
                async (TData currentStorage) =>
                {
                    var result = await onUpdate(currentStorage,
                        async (documentToSave, success, modified) =>
                        {
                            try
                            {
                                await UpdateIfNotModifiedAsync(documentToSave);
                                return success();
                            }
                            catch (StorageException ex)
                            {
                                if (ex.IsProblemTimeout())
                                {
                                    return await await onTimeout(ex.RequestInformation.HttpStatusCode, ex,
                                        () => UpdateAsync(id, onUpdate, onNotFound, onTimeout),
                                        () => { throw new Exception(); });
                                }
                                if (ex.IsProblemPreconditionFailed())
                                    return await UpdateAsync(id, onUpdate, onNotFound, onTimeout);
                                throw;
                            }
                        });
                    return result;
                },
                () => Task.FromResult(onNotFound()));
        }

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
                    return await RepeatAtomic(
                        async () =>
                        {
                            return await atomicModifier(document, async (updateDocumentWith) =>
                            {
                                await UpdateIfNotModifiedAsync(updateDocumentWith);
                            });
                        },
                        async () => await CreateOrUpdateAtomicAsync(id, atomicModifier, onTimeout));
                },
                async () =>
                {
                    return await RepeatAtomic(
                        async () =>
                        {
                            return await atomicModifier(default(TData), async (createDocumentWith) =>
                            {
                                createDocumentWith.SetId(id);
                                await CreateAsync(createDocumentWith);
                            });
                        },
                        async () => await CreateOrUpdateAtomicAsync(id, atomicModifier, onTimeout));
                },
                onTimeout);
            return result;
        }
        
        public delegate TResult CreateSuccessDelegate<TResult>();
        public async Task<TResult> CreateAsync<TResult, TData>(Guid id, TData document,
            CreateSuccessDelegate<TResult> onSuccess,
            AlreadyExitsDelegate<TResult> onAlreadyExists,
            RetryDelegate onTimeout = default(RetryDelegate))
            where TData : class, ITableEntity
        {
            if (default(RetryDelegate) == onTimeout)
                onTimeout = GetRetryDelegate();

            document.SetId(id);
            
            while (true)
            {
                try
                {
                    await CreateAsync(document);
                    return onSuccess();
                }
                catch (StorageException ex)
                {
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
                
            }
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

        #region Locked Update

        public delegate Task<bool> ConditionForLockingDelegateAsync<TDocument>(
            TDocument lockedDocument, SaveDocumentDelegate<TDocument> updateDocumentCallback);

        private ConditionForLockingDelegateAsync<TDocument> CreateCompositeConditionForLockingCallback<TDocument>(
            Expression<Predicate<TDocument>> lockedPropertyExpression,
            ConditionForLockingDelegateAsync<TDocument> conditionForLockingCallback,
            out Func<TDocument, TDocument> unlockCallback)
        {
            // decompile lock property expression to propertyInfo
            var lockedPropertyMember = ((MemberExpression)lockedPropertyExpression.Body).Member;
            var fieldInfo = lockedPropertyMember as FieldInfo;
            var propertyInfo = lockedPropertyMember as PropertyInfo;

            ConditionForLockingDelegateAsync<TDocument> compositeConditionForLockingCallback =
                async (lockedDocument, updateDocumentCallback) =>
                {
                    var locked = (bool)(fieldInfo != null ? fieldInfo.GetValue(lockedDocument) : propertyInfo.GetValue(lockedDocument));
                    if (locked)
                        return false;

                    if (fieldInfo != null) fieldInfo.SetValue(lockedDocument, true);
                    else propertyInfo.SetValue(lockedDocument, true);

                    return await conditionForLockingCallback(lockedDocument, updateDocumentCallback);
                };

            unlockCallback = (documentToUnlock) =>
            {
                if (fieldInfo != null) fieldInfo.SetValue(documentToUnlock, false);
                else propertyInfo?.SetValue(documentToUnlock, false);
                return documentToUnlock;
            };

            return compositeConditionForLockingCallback;
        }

        public delegate Task<TResult> WhileLockedDelegateAsync2<TDocument, TResult>(
            TDocument lockedDocument, SaveDocumentDelegate<TDocument> saveDocumentCallback);

        public async Task<TResult> LockedCreateOrUpdateAsync<TDocument, TResult>(Guid id,
                Expression<Predicate<TDocument>> lockedPropertyExpression,
                WhileLockedDelegateAsync2<TDocument, TResult> whileLockedCallback,
                Func<TResult> lockFailedCallback)
            where TDocument : TableEntity
        {
            Func<TDocument, TDocument> unlockMethod;
            var compositeConditionForLockingCallback = CreateCompositeConditionForLockingCallback(
                lockedPropertyExpression, (document, save) => { save(document); return Task.FromResult(true); }, out unlockMethod);

            var lookupMethod = CreateLookupMethodCallbackUpdateAtomic<TDocument>(id);
            
            return await LockedCreateOrUpdateAsync(id,
                compositeConditionForLockingCallback,
                whileLockedCallback,
                lockFailedCallback, unlockMethod,
                lookupMethod);
        }

        public async Task<TResult> LockedCreateOrUpdateAsync<TDocument, TResult>(Guid id,
                Expression<Predicate<TDocument>> lockedPropertyExpression,
                ConditionForLockingDelegateAsync<TDocument> conditionForLockingCallback,
                WhileLockedDelegateAsync2<TDocument, TResult> whileLockedCallback,
                Func<TResult> lockFailedCallback)
            where TDocument : TableEntity
        {
            Func<TDocument, TDocument> unlockMethod;
            var compositeConditionForLockingCallback = CreateCompositeConditionForLockingCallback(
                lockedPropertyExpression, conditionForLockingCallback, out unlockMethod);

            var lookupMethod = CreateLookupMethodCallbackUpdateAtomic<TDocument>(id);
            
            return await LockedCreateOrUpdateAsync(id,
                compositeConditionForLockingCallback,
                whileLockedCallback,
                lockFailedCallback, unlockMethod,
                lookupMethod);
        }

        internal delegate Task<TResult> LookupMethodDelegateAsync<TDocument, TResult>(
            TDocument currentStoredDocument, Action<TDocument> saveDocumentCallback);
        internal delegate Task<TResult> LookupMethodCallbackDelegateAsync<TDocument, TResult>(
            LookupMethodDelegateAsync<TDocument, TResult> lookupCallback);

        private LookupMethodCallbackDelegateAsync<TDocument, bool> CreateLookupMethodCallbackUpdateAtomic<TDocument>(Guid id)
            where TDocument : TableEntity
        {
            LookupMethodCallbackDelegateAsync<TDocument, bool> lookupMethod =
                async (lookupMethodCallback) =>
                {
                    return await CreateOrUpdateAtomicAsync<TDocument>(id,
                        async (currentStoredDocument) =>
                        {
                            var documentToSave = default(TDocument);
                            await lookupMethodCallback(currentStoredDocument,
                                (newDocument) => documentToSave = newDocument);
                            return documentToSave;
                        });
                };
            return lookupMethod;
        }

        internal async Task<TResult> LockedCreateOrUpdateAsync<TDocument, TResult>(Guid id,
                ConditionForLockingDelegateAsync<TDocument> mutateDocumentToLockedStateCallback,
                WhileLockedDelegateAsync2<TDocument, TResult> whileLockedCallback,
                Func<TResult> lockFailedCallback,
                Func<TDocument, TDocument> unlockCallback,
                LookupMethodCallbackDelegateAsync<TDocument, bool> createOrUpdateCallback)
            where TDocument : TableEntity
        {
            var result = await retryPolicy.RetryAsync(
                async (terminateCallback) =>
                {
                    #region Do Idempotent locking

                    var lockedDocument = default(TDocument);
                    var didUpdateStorage = await createOrUpdateCallback(async (currentStoredDocument, saveDocumentCallback) =>
                    {
                        if (default(TDocument) == currentStoredDocument)
                            throw new RecordNotFoundException(); // TODO: since this is _CREATE_ or update this should never happen, log accordingly

                        if (!await mutateDocumentToLockedStateCallback(currentStoredDocument,
                            async (updatedDocument) =>
                            {
                                // TODO: Rework this
                                await Task.FromResult(true);
                                lockedDocument = updatedDocument;
                            }))
                        {
                            var failedResult = lockFailedCallback();
                            terminateCallback(failedResult);
                            return false;
                        }

                        saveDocumentCallback(lockedDocument);
                        return true;
                    });

                    if (!didUpdateStorage)
                        return;

                    #endregion
                    
                    try
                    {
                        var documentToSave = default(TDocument);
                        var retryResult = await whileLockedCallback(lockedDocument, async updatedDocument =>
                        {
                            // TODO: Rework this
                            await Task.FromResult(true);
                            documentToSave = updatedDocument;
                        });
                        await Unlock<TDocument>(id, (storedDocument) => unlockCallback(documentToSave));
                        terminateCallback(retryResult);
                    }
                    catch (Exception ex)
                    {
                        await Unlock<TDocument>(id, (storedDocument) => unlockCallback(storedDocument));
                        throw ex;
                    }
                },
                () => lockFailedCallback());
            return result;
        }

        private async Task Unlock<TDocument>(Guid id, Func<TDocument, TDocument> mutateEntityToSaveAction)
            where TDocument : TableEntity
        {
            await retryPolicy.RetryAsync(
                async (onSuccess) =>
                {
                    var unlockSucceeded = await UpdateAtomicAsync<TDocument>(id,
                        lockedEntityAtomic => mutateEntityToSaveAction(lockedEntityAtomic));
                    if(unlockSucceeded)
                        onSuccess(true);
                },
                () => false);
        }

        #endregion

        public delegate Task<TResult> UnlockAndSaveDelegate<TDocument, TResult>(TResult result,
            Func<TDocument, Action<TDocument>, Task> callback);
        public delegate Task<TResult> ConditionForLockingDelegate<TDocument, TResult>(TDocument document,
            Func<Task<TResult>> proceedToLock,
            Func<TResult> doNotLock);
        public delegate Task<TResult> WhileLockedDelegateAsync<TDocument, TResult>(TDocument document,
            UnlockAndSaveDelegate<TDocument, TResult> unlockAndSave,
            Func<TResult, Task<TResult>> unlock);
        public async Task<TResult> LockedUpdateAsync<TDocument, TResult>(Guid id,
                Expression<Func<TDocument, bool>> lockedPropertyExpression,
                WhileLockedDelegateAsync<TDocument, TResult> success,
                Func<TResult> notLocked,
                Func<TResult> neverLocked,
                Func<TResult> notFound,
                RetryDelegateAsync<Task<TResult>> onTimeout = default(RetryDelegateAsync<Task<TResult>>))
            where TDocument : TableEntity
        {
            // decompile lock property expression to propertyInfo for easy use later
            var lockedPropertyMember = ((MemberExpression)lockedPropertyExpression.Body).Member;
            var fieldInfo = lockedPropertyMember as FieldInfo;
            var propertyInfo = lockedPropertyMember as PropertyInfo;

            if (default(RetryDelegateAsync<Task<TResult>>) == onTimeout)
                onTimeout = GetRetryDelegateAsync<Task<TResult>>();

            return await await this.UpdateAsync<TDocument, Task<TResult>>(id,
                async (document, save) =>
                {
                    #region convert unlocked document to locked state

                    var locked = (bool)(fieldInfo != null ? fieldInfo.GetValue(document) : propertyInfo.GetValue(document));
                    if (locked)
                    {
                        return await onTimeout(409, new Exception("Resource is locked"),
                            async () => await LockedUpdateAsync(id, lockedPropertyExpression, success, notLocked, neverLocked, notFound, onTimeout),
                            () => Task.FromResult(neverLocked()));
                    }
                    if (fieldInfo != null) fieldInfo.SetValue(document, true);
                    else propertyInfo.SetValue(document, true);

                    #endregion

                    return await save(document,
                        async () =>
                        {
                            try
                            {
                                return await success(document,
                                    async (result, callback) =>
                                    {
                                        return await Unlock<TDocument, TResult>(id,
                                            fieldInfo, propertyInfo,
                                            (TDocument documentToUnlock) =>
                                            {
                                                var documentToSave = documentToUnlock;
                                                callback(documentToUnlock,
                                                    (documentToSaveUpdated) =>
                                                    {
                                                        documentToSave = documentToSaveUpdated;
                                                    });
                                                return documentToUnlock;
                                            },
                                            () => result,
                                            () => { throw new Exception("Unlock failed"); }); // TODO: Log
                                    },
                                    async (result) =>
                                    {
                                        return await Unlock<TDocument, TResult>(id,
                                            fieldInfo, propertyInfo,
                                            (TDocument documentToUnlock) => documentToUnlock,
                                            () => result,
                                            () => { throw new Exception("Unlock failed"); }); // TODO: Log
                                    });
                            }
                            catch (Exception ex)
                            {
                                throw await Unlock<TDocument, Exception>(id, fieldInfo, propertyInfo, (documentToUnlock) => documentToUnlock,
                                    () => ex,
                                    () => ex);
                            }
                        },
                        async () =>
                        {
                            return await await onTimeout(409, new Exception("Resource is locked"),
                               async () => await LockedUpdateAsync(id, lockedPropertyExpression, success, notLocked, neverLocked, notFound, onTimeout),
                               () => Task.FromResult(neverLocked()));
                        });
                },
                () => Task.FromResult(notFound()));
        }

        private async Task<TResult> Unlock<TDocument, TResult>(Guid id,
            FieldInfo fieldInfo, PropertyInfo propertyInfo,
            Func<TDocument, TDocument> mutateEntityToSaveAction,
            Func<TResult> onSuccess,
            Func<TResult> onFailure)
            where TDocument : TableEntity
        {
            var unlockSucceeded = await UpdateAtomicAsync<TDocument>(id,
                lockedEntityAtomic =>
                {
                    lockedEntityAtomic.SetFieldOrProperty(false, fieldInfo, propertyInfo);
                    mutateEntityToSaveAction(lockedEntityAtomic);
                    return lockedEntityAtomic;
                });
            if (unlockSucceeded)
                return onSuccess();
            return onFailure();
        }
    }
}
