using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BlackBarLabs.Linq.Async;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;

namespace BlackBarLabs.Persistence.Azure.StorageTables.Backups
{
    public static class Container
    {
        private static readonly BlobRequestOptions BlobOptions =
            new BlobRequestOptions
            {
                RetryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(4), 10)
            };

        private static readonly AccessCondition BlobCondition = AccessCondition.GenerateEmptyCondition();

        public delegate void BlobProgressDelegate(int completed, int total);

        public static async Task<TResult> CopyAsync<TResult>(this CloudBlobContainer sourceContainer, CloudBlobClient targetClient, 
                DateTime started, TimeSpan sourceAccessWindow,
            BlobProgressDelegate onCopyProgress,
            Func<CloudBlobContainer, TResult> onSuccess,
            Func<string, TResult> onFailure)
        {
            return await await sourceContainer.PrepareContainerForCopyAsync(targetClient,
                started, sourceAccessWindow,
                async (targetContainer, accessKey, releaseSourceAsync) =>
                {
                    try
                    {
                        var existingTargetBlobs = await targetContainer.FindBlobsAsync(blobs => blobs);
                        var total = await await sourceContainer.FindBlobsAsync(
                            async (sourceBlobs) =>
                            {
                                return await sourceBlobs.Select(
                                        async sourceBlob =>
                                        {
                                            return await sourceBlob.StartBackgroundCopyAsync(targetContainer,
                                                accessKey, existingTargetBlobs, targetBlob => targetBlob != null);
                                        })
                                    .WhenAllAsync(25)
                                    .WhereAsync(item => item)
                                    .ToArrayAsync();
                            });
                        return await await targetContainer.WaitForBlobsToCopyAsync(onCopyProgress, total.Length,
                            async () =>
                            {
                                await releaseSourceAsync();
                                return onSuccess(targetContainer);
                            });
                    }
                    catch (Exception e)
                    {
                        await releaseSourceAsync();
                        return onFailure(e.Message);
                    }
                });
        }

        public static async Task<TResult> FindContainersAsync<TResult>(this CloudBlobClient sourceClient, Func<CloudBlobContainer[], TResult> onSuccess)
        {
            var context = new OperationContext();
            BlobContinuationToken token = null;
            var containers = new List<CloudBlobContainer>();
            while (true)
            {
                var segment = await sourceClient.ListContainersSegmentedAsync(null, ContainerListingDetails.All, null, token, BlobOptions, context);
                var results = segment.Results.ToArray();
                containers.AddRange(results);
                token = segment.ContinuationToken;
                if (null == token)
                    return onSuccess(containers.ToArray());
            }
        }

        private static async Task<TResult> PrepareContainerForCopyAsync<TResult>(this CloudBlobContainer sourceContainer,
           CloudBlobClient blobClient, DateTime started, TimeSpan sourceAccessWindow,
           Func<CloudBlobContainer, string, Func<Task>, TResult> onSuccess)
        {
            var targetContainerName = sourceContainer.GetNameForCopy(started);
            var targetContainer = blobClient.GetContainerReference(targetContainerName);
            var context = new OperationContext();
            var exists = await targetContainer.ExistsAsync(BlobOptions, context);
            if (!exists)
            {
                var createPermissions = await sourceContainer.GetPermissionsAsync(BlobCondition, BlobOptions, context);
                await targetContainer.CreateAsync(createPermissions.PublicAccess, BlobOptions, context);

                var metadataModified = false;
                foreach (var item in sourceContainer.Metadata)
                {
                    if (!targetContainer.Metadata.ContainsKey(item.Key) || targetContainer.Metadata[item.Key] != item.Value)
                    {
                        targetContainer.Metadata[item.Key] = item.Value;
                        metadataModified = true;
                    }
                }
                if (metadataModified)
                    await targetContainer.SetMetadataAsync(BlobCondition, BlobOptions, context);
            }

            var permissions = await sourceContainer.GetPermissionsAsync(BlobCondition, BlobOptions, context);
            permissions.SharedAccessPolicies.Clear();
            var accessKey = $"{targetContainerName}-access";
            permissions.SharedAccessPolicies.Add(accessKey, new SharedAccessBlobPolicy
                {
                    SharedAccessExpiryTime = DateTime.UtcNow.Add(sourceAccessWindow),
                    Permissions = SharedAccessBlobPermissions.Read
                });
            await sourceContainer.SetPermissionsAsync(permissions, BlobCondition, BlobOptions, context);
            return onSuccess(targetContainer, accessKey, 
                async () =>
                {
                    permissions.SharedAccessPolicies.Clear();
                    await sourceContainer.SetPermissionsAsync(permissions, BlobCondition, BlobOptions, context);
                });
        }

        private static async Task<TResult> FindBlobsAsync<TResult>(this CloudBlobContainer container, Func<CloudBlob[], TResult> onSuccess)
        {
            var context = new OperationContext();
            BlobContinuationToken token = null;
            var blobs = new List<IListBlobItem>();
            while (true)
            {
                var segment = await container.ListBlobsSegmentedAsync(null, true, BlobListingDetails.All, null, token, BlobOptions, context);
                var results = segment.Results.ToArray();
                blobs.AddRange(results);
                token = segment.ContinuationToken;
                if (null == token)
                    return onSuccess(blobs.Cast<CloudBlob>().ToArray());
            }
        }

        private static async Task<TResult> StartBackgroundCopyAsync<TResult>(this CloudBlob sourceBlob, CloudBlobContainer targetContainer, string accessKey, CloudBlob[] existingTargetBlobs,
            Func<CloudBlob, TResult> onSuccess)
        {
            var target = existingTargetBlobs.FirstOrDefault(tb => tb.Uri.AbsolutePath == sourceBlob.Uri.AbsolutePath);
            if (null != target && target.Properties.ContentMD5 == sourceBlob.Properties.ContentMD5)
                return onSuccess(null);

            target = targetContainer.GetReference(sourceBlob.BlobType, sourceBlob.Name);
            var sas = sourceBlob.GetSharedAccessSignature(null, accessKey);
            var context = new OperationContext();
            await target.StartCopyAsync(new Uri(sourceBlob.Uri + sas), BlobCondition, BlobCondition, BlobOptions, context);
            return onSuccess(target);
        }

        private static async Task<TResult> WaitForBlobsToCopyAsync<TResult>(this CloudBlobContainer targetContainer,
            BlobProgressDelegate onCopyProgress, int total, Func<TResult> onSuccess)
        {
            while (true)
            {
                var stillRunningList = await await FindBlobsAsync(targetContainer,
                    async blobs =>
                    {
                        return await blobs.Select(
                            async blob =>
                            {
                                switch (blob.CopyState?.Status ?? CopyStatus.Invalid)
                                {
                                    case CopyStatus.Pending:
                                        return true;
                                    case CopyStatus.Failed:
                                    case CopyStatus.Aborted:
                                        var context = new OperationContext();
                                        await blob.StartCopyAsync(blob.CopyState.Source, BlobCondition,
                                            BlobCondition, BlobOptions, context);
                                        return true;
                                    default:
                                        return false;
                                }
                            })
                            .WhenAllAsync();
                    });
                var stillRunningCount = stillRunningList.Count(x => x);
                if (stillRunningCount == 0)
                    return onSuccess();

                onCopyProgress(stillRunningCount, total);
                await Task.Delay(TimeSpan.FromMinutes(5));
            }
        }
    }
}
