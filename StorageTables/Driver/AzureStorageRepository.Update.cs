using System;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using BlackBarLabs.Extensions;
using EastFive.Extensions;
using EastFive.Azure.StorageTables.Driver;

namespace BlackBarLabs.Persistence.Azure.StorageTables
{
    public partial class AzureStorageRepository
    {
        public delegate TResult UpdateDelegate<TData, TResult>(TData currentStorage, SaveDocumentDelegate<TData> saveNew);
        public async Task<TResult> UpdateAsync<TData, TResult>(Guid documentId,
            UpdateDelegate<TData, Task<TResult>> onUpdate,
            Func<TResult> onNotFound,
            RetryDelegateAsync<Task<TResult>> onTimeoutAsync = default(RetryDelegateAsync<Task<TResult>>))
            where TData : class, ITableEntity
        {
            var rowKey = documentId.AsRowKey();
            var partitionKey = rowKey.GeneratePartitionKey();
            return await UpdateAsync(rowKey, partitionKey, onUpdate, onNotFound);
        }

        public async Task<TResult> UpdateAsync<TData, TResult>(Guid documentId, string partitionKey,
            UpdateDelegate<TData, Task<TResult>> onUpdate,
            Func<TResult> onNotFound,
            RetryDelegateAsync<Task<TResult>> onTimeoutAsync = default(RetryDelegateAsync<Task<TResult>>))
            where TData : class, ITableEntity
        {
            var rowKey = documentId.AsRowKey();
            return await UpdateAsync(rowKey, partitionKey, onUpdate, onNotFound);
        }

        public async Task<TResult> UpdateAsync<TData, TResult>(string rowKey, string partitionKey,
            UpdateDelegate<TData, Task<TResult>> onUpdate,
            Func<TResult> onNotFound,
            RetryDelegateAsync<Task<TResult>> onTimeoutAsync = default(RetryDelegateAsync<Task<TResult>>))
            where TData : class, ITableEntity
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
                                () => false.ToTask(),
                                async () =>
                                {
                                    if (onTimeoutAsync.IsDefaultOrNull())
                                        onTimeoutAsync = GetRetryDelegateContentionAsync<Task<TResult>>();
                                    
                                    resultGlobal = await await onTimeoutAsync(
                                        async () => await UpdateAsync(rowKey, partitionKey, onUpdate, onNotFound, onTimeoutAsync),
                                        (numberOfRetries) => { throw new Exception("Failed to gain atomic access to document after " + numberOfRetries + " attempts"); });
                                    return true;
                                },
                                onTimeout: GetRetryDelegate());
                        });
                    return useResultGlobal ? resultGlobal : resultLocal;
                },
                () => Task.FromResult(onNotFound()),
                default(Func<ExtendedErrorInformationCodes, string, Task<TResult>>),
                GetRetryDelegate());
        }
        
    }
}
