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
        public delegate TResult UpdateDelegate<TData, TResult>(TData currentStorage, SaveDocumentDelegate<TData> saveNew);
        public async Task<TResult> UpdateAsync<TData, TResult>(Guid id,
            UpdateDelegate<TData, Task<TResult>> onUpdate,
            NotFoundDelegate<TResult> onNotFound,
            RetryDelegateAsync<Task<TResult>> onTimeoutAsync = default(RetryDelegateAsync<Task<TResult>>))
            where TData : class, ITableEntity
        {
            if (default(RetryDelegateAsync<Task<TResult>>) == onTimeoutAsync)
                onTimeoutAsync = GetRetryDelegateContentionAsync<Task<TResult>>();

            return await await FindByIdAsync(id,
                async (TData currentStorage) =>
                {
                    var resultGlobal = default(TResult);
                    var useResultGlobal = false;
                    var resultLocal = await onUpdate.Invoke(currentStorage,
                        async (documentToSave) =>
                        {
                            useResultGlobal = await await UpdateIfNotModifiedAsync(documentToSave,
                                () => false.ToTask(),
                                async () =>
                                {
                                    resultGlobal = await await onTimeoutAsync(
                                        async () => await UpdateAsync(id, onUpdate, onNotFound, onTimeoutAsync),
                                        (numberOfRetries) => { throw new Exception("Failed to gain atomic access to document after " + numberOfRetries + " attempts"); });
                                    return true;
                                },
                                GetRetryDelegate());
                        });
                    return useResultGlobal ? resultGlobal : resultLocal;
                },
                () => Task.FromResult(onNotFound()),
                GetRetryDelegate());
        }

        public delegate Task<TResult> SaveDocumentDelegateAsync<TDocument, TResult>(TDocument documentToSave,
            Func<TResult> success, Func<TResult> modified);
        public delegate Task<TResult> UpdateDelegateAsync<TDocument, TResult>(TDocument currentStorage,
            SaveDocumentDelegateAsync<TDocument, TResult> saveNew);
        [Obsolete("UpdateAsync with UpdateDelegateAsync parameter is deprecated, please use UpdateAsync with UpdateDelegate<TData, Task<TResult>> instead.")]
        public async Task<TResult> UpdateAsync<TData, TResult>(Guid id,
            UpdateDelegateAsync<TData, TResult> onUpdate,
            NotFoundDelegate<TResult> onNotFound,
            RetryDelegateAsync<Task<TResult>> onTimeout = default(RetryDelegateAsync<Task<TResult>>))
            where TData : class, ITableEntity
        {
            if (default(RetryDelegateAsync<Task<TResult>>) == onTimeout)
                onTimeout = GetRetryDelegateContentionAsync<Task<TResult>>();

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
                                    return await await onTimeout(
                                        () => UpdateAsync(id, onUpdate, onNotFound, onTimeout),
                                        (numberOfRetries) => { throw new Exception(); });
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

        public delegate Task UpdateSuccessSaveDocumentDelegateAsync<TDocument>(Func<TDocument, TDocument> success);
        public delegate TResult UpdateSuccessDelegateAsync<TDocument, TResult>(TDocument currentStorage,
            UpdateSuccessSaveDocumentDelegateAsync<TDocument> saveNew);
        [Obsolete("UpdateAsync with UpdateSuccessDelegateAsync parameter is deprecated, please use UpdateAsync with UpdateDelegate<TData, Task<TResult>> instead.")]
        public async Task<TResult> UpdateAsync<TDocument, TResult>(Guid id,
            UpdateSuccessDelegateAsync<TDocument, TResult> onUpdate,
            NotFoundDelegate<TResult> onNotFound,
            RetryDelegateAsync<TResult> onTimeout = default(RetryDelegateAsync<TResult>))
            where TDocument : class, ITableEntity
        {
            if (onTimeout.IsDefaultOrNull())
                onTimeout = GetRetryDelegateContentionAsync<TResult>();

            return await FindByIdAsync(id,
                (TDocument currentStorage) =>
                {
                    var result = onUpdate(currentStorage,
                        async (documentSaveCallback) =>
                        {
                            while (true)
                            {
                                var newDoc = documentSaveCallback(currentStorage);
                                try
                                {
                                    await UpdateIfNotModifiedAsync(newDoc);
                                    break;
                                }
                                catch (StorageException ex)
                                {
                                    if (ex.IsProblemTimeout())
                                    {
                                        // TODO: Implement this
                                        continue;
                                    }
                                    if (ex.IsProblemPreconditionFailed())
                                        continue;
                                    throw;
                                }
                            }
                        });
                    return result;
                },
                () => onNotFound());
        }
    }
}
