using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using BlackBarLabs.Extensions;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;

namespace BlackBarLabs.Persistence.Azure.StorageTables.Backups
{
    public enum BlobStatus
    {
        CopySuccessful,
        Running,
        ShouldRetry,
        Failed
    }

    public struct ContainerStatistics
    {
        public int successes;
        public KeyValuePair<CloudBlob, BlobStatus>[] retries;
        public KeyValuePair<CloudBlob, BlobStatus>[] failures;
    }

    public struct BlobCopyOptions
    {
        public TimeSpan accessPeriod;
        public int blobsInBatch;
        public TimeSpan checkCopyCompleteAfter;
        public int copyRetries;

        public static readonly BlobCopyOptions Default = new BlobCopyOptions
        {
            accessPeriod = TimeSpan.FromMinutes(30),
            blobsInBatch = 200,
            checkCopyCompleteAfter = TimeSpan.FromSeconds(5),
            copyRetries = 10
        };
    }

    public static class Container
    {
        private struct BlobAccess
        {
            public string key;
            public DateTime expiresUtc;
        }

        private static readonly BlobRequestOptions RetryOptions =
            new BlobRequestOptions
            {
                RetryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(4), 10)
            };

        private static readonly AccessCondition EmptyCondition = AccessCondition.GenerateEmptyCondition();

        private static OperationContext CreateContext()
        {
            return new OperationContext();
        }

        public static async Task<TResult> FindAllContainersAsync<TResult>(this CloudBlobClient sourceClient, Func<CloudBlobContainer[], TResult> onSuccess)
        {
            var context = CreateContext();
            BlobContinuationToken token = null;
            var containers = new List<CloudBlobContainer>();
            while (true)
            {
                try
                {
                    var segment = await sourceClient.ListContainersSegmentedAsync(null, ContainerListingDetails.All, null, token, RetryOptions, context);
                    var results = segment.Results.ToArray();
                    containers.AddRange(results);
                    token = segment.ContinuationToken;
                    if (null == token)
                        return onSuccess(containers.ToArray());
                }
                catch (Exception e)
                {
                    throw e;
                }
            }
        }

        public static async Task<KeyValuePair<string, ContainerStatistics>> CopyContainerAsync(this CloudBlobContainer sourceContainer, CloudBlobClient targetClient, DateTime whenStartedUtc, BlobCopyOptions copyOptions)
        {
            var targetContainerName = sourceContainer.GetNameForCopy(whenStartedUtc);
            return await await sourceContainer.CreateOrUpdateTargetContainerForCopyAsync(targetClient, targetContainerName,
                async (targetContainer, existingTargetBlobs, renewAccessAsync, releaseAccessAsync) =>
                {
                    try
                    {
                        var stat = await await sourceContainer.FindAllBlobsAsync(
                            blobs => blobs.CopyBlobsWithContainerKeyAsync(targetContainer, existingTargetBlobs, copyOptions.checkCopyCompleteAfter, () => renewAccessAsync(copyOptions.accessPeriod), copyOptions.blobsInBatch, copyOptions.copyRetries));
                        return sourceContainer.Name.PairWithValue(stat);
                    }
                    catch (Exception e)
                    {
                        var name = $"{sourceContainer.Name} had error {e.Message}";
                        return name.PairWithValue(default(ContainerStatistics));
                    }
                    finally
                    {
                        await releaseAccessAsync();
                    }
                });
        }

        private static string GetNameForCopy(this CloudBlobContainer sourceContainer, DateTime date)
        {
            return $"{date:yyyyMMddHHmmss}-{sourceContainer.ServiceClient.Credentials.AccountName}-{sourceContainer.Name}";
        }

        private static async Task<TResult> CreateOrUpdateTargetContainerForCopyAsync<TResult>(this CloudBlobContainer sourceContainer,
           CloudBlobClient blobClient, string targetContainerName,
           Func<CloudBlobContainer, CloudBlob[], Func<TimeSpan, Task<BlobAccess>>, Func<Task>, TResult> onSuccess)
        {
            try
            {
                var targetContainer = blobClient.GetContainerReference(targetContainerName);
                var context = CreateContext();
                var exists = await targetContainer.ExistsAsync(RetryOptions, context);
                if (!exists)
                {
                    var createPermissions = await sourceContainer.GetPermissionsAsync(EmptyCondition, RetryOptions, context);
                    await targetContainer.CreateAsync(createPermissions.PublicAccess, RetryOptions, context);

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
                        await targetContainer.SetMetadataAsync(EmptyCondition, RetryOptions, context);
                }
                var existingTargetItems = await targetContainer.FindAllBlobsAsync(blobs => blobs);

                return onSuccess(targetContainer,
                    existingTargetItems,
                    async (sourceAccessWindow) =>
                    {
                        var renewContext = CreateContext();
                        var permissions = await sourceContainer.GetPermissionsAsync(EmptyCondition, RetryOptions, renewContext);
                        permissions.SharedAccessPolicies.Clear();
                        var access = new BlobAccess
                        {
                            key = $"{targetContainerName}-access",
                            expiresUtc = DateTime.UtcNow.Add(sourceAccessWindow)
                        };
                        permissions.SharedAccessPolicies.Add(access.key, new SharedAccessBlobPolicy
                        {
                            SharedAccessExpiryTime = access.expiresUtc,
                            Permissions = SharedAccessBlobPermissions.Read
                        });
                        await sourceContainer.SetPermissionsAsync(permissions, EmptyCondition, RetryOptions, renewContext);
                        return access;
                    },
                    async () =>
                    {
                        var releaseContext = CreateContext();
                        var permissions = await sourceContainer.GetPermissionsAsync(EmptyCondition, RetryOptions, releaseContext);
                        permissions.SharedAccessPolicies.Clear();
                        await sourceContainer.SetPermissionsAsync(permissions, EmptyCondition, RetryOptions, releaseContext);
                    });
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        private static async Task<TResult> FindAllBlobsAsync<TResult>(this CloudBlobContainer container, Func<CloudBlob[], TResult> onSuccess)
        {
            var context = CreateContext();
            BlobContinuationToken token = null;
            var blobs = new List<IListBlobItem>();
            while (true)
            {
                var segment = await container.ListBlobsSegmentedAsync(null, true,
                    BlobListingDetails.UncommittedBlobs, null, token, RetryOptions, context);
                var results = segment.Results.ToArray();
                blobs.AddRange(results);
                token = segment.ContinuationToken;
                if (null == token)
                    return onSuccess(blobs.Cast<CloudBlob>().ToArray());
            }
        }

        private static async Task<ContainerStatistics> CopyBlobsWithContainerKeyAsync(this CloudBlob[] sourceBlobs, CloudBlobContainer targetContainer, CloudBlob[] existingTargetBlobs, TimeSpan checkCopyCompleteAfter, Func<Task<BlobAccess>> renewAccessAsync, int blobsInBatch, int copyRetries)
        {
            BlobAccess access = default(BlobAccess);
            var statistics = await sourceBlobs
                .Select((x, index) => new { x, index })
                .GroupBy(x => x.index / blobsInBatch, y => y.x)
                .Aggregate(
                    new ContainerStatistics
                    {
                        successes = 0,
                        retries = new KeyValuePair<CloudBlob, BlobStatus>[] { },
                        failures = new KeyValuePair<CloudBlob, BlobStatus>[] { }
                    }.ToTask(),
                    async (statsTask, group) =>
                    {
                        var stats = await statsTask;
                        if (access.expiresUtc < DateTime.UtcNow)
                            access = await renewAccessAsync();
                        var pairs = await group
                            .ToArray()
                            .Select(blob => blob.StartCopyAndWaitForCompletionAsync(targetContainer, access.key, existingTargetBlobs, checkCopyCompleteAfter))
                            .WhenAllAsync();
                        return new ContainerStatistics
                        {
                            successes = stats.successes + pairs.Count(pair => pair.Value == BlobStatus.CopySuccessful),
                            retries = stats.retries.Concat(pairs.Where(pair => pair.Value == BlobStatus.ShouldRetry)).ToArray(),
                            failures = stats.failures.Concat(pairs.Where(pair => pair.Value == BlobStatus.Failed)).ToArray(),
                        };
                    });
            if (!statistics.retries.Any() || copyRetries <= 0)
                return statistics;

            var recursiveStatistics = await CopyBlobsWithContainerKeyAsync(statistics.retries.Select(p => p.Key).ToArray(), targetContainer, existingTargetBlobs, checkCopyCompleteAfter, renewAccessAsync, blobsInBatch, --copyRetries);

            return new ContainerStatistics
            {
                successes = statistics.successes + recursiveStatistics.successes,
                retries = recursiveStatistics.retries,
                failures = statistics.failures.Concat(recursiveStatistics.failures).ToArray()
            };
        }

        private static async Task<KeyValuePair<CloudBlob,BlobStatus>> StartCopyAndWaitForCompletionAsync(this CloudBlob sourceBlob, CloudBlobContainer targetContainer, string accessKey, CloudBlob[] existingTargetBlobs, TimeSpan checkCopyCompleteAfter)
        {
            return await await sourceBlob.StartBackgroundCopyAsync(targetContainer, accessKey, existingTargetBlobs,
                async (started, progressAsync) =>
                {
                    try
                    {
                        while (true)
                        {
                            if (started)
                                await Task.Delay(checkCopyCompleteAfter);
                            var state = await progressAsync();
                            if (BlobStatus.Running == state)
                                continue;
                            return sourceBlob.PairWithValue(state);
                        }
                    }
                    catch (Exception e)
                    {
                        var inner = e;
                        while (inner.InnerException != null)
                            inner = inner.InnerException;

                        // This catches when our shared access key has expired after the copy has begin
                        var status = inner.Message.Contains("could not finish the operation within specified timeout") ? BlobStatus.ShouldRetry : BlobStatus.Failed;
                        return sourceBlob.PairWithValue(status);
                    }
                });
        }

        private static async Task<TResult> StartBackgroundCopyAsync<TResult>(this CloudBlob sourceBlob, CloudBlobContainer targetContainer, string accessKey, CloudBlob[] existingTargetBlobs,
            Func<bool, Func<Task<BlobStatus>>, TResult> onSuccess) // started, progressAsync
        {
            var started = true;
            var notStarted = !started;
            var target = existingTargetBlobs.FirstOrDefault(tb => tb.Name == sourceBlob.Name);
            if (null != target && target.Properties.ContentMD5 == sourceBlob.Properties.ContentMD5 && target.Properties.Length == sourceBlob.Properties.Length)
                return onSuccess(notStarted, () => BlobStatus.CopySuccessful.ToTask());

            target = targetContainer.GetReference(sourceBlob.BlobType, sourceBlob.Name);
            var sas = sourceBlob.GetSharedAccessSignature(null, accessKey);
            try
            {
                await target.StartCopyAsync(new Uri(sourceBlob.Uri + sas), EmptyCondition, EmptyCondition, RetryOptions, CreateContext());
                return onSuccess(started,
                    async () =>
                    {
                        var blob = (await targetContainer.GetBlobReferenceFromServerAsync(sourceBlob.Name, AccessCondition.GenerateEmptyCondition(), RetryOptions, CreateContext())) as CloudBlob;
                        var copyStatus = blob.CopyState?.Status ?? CopyStatus.Invalid;
                        if (CopyStatus.Success == copyStatus)
                            return BlobStatus.CopySuccessful;
                        if (CopyStatus.Pending == copyStatus)
                            return BlobStatus.Running;
                        // This catches when the shared access key expired before the blob finished copying
                        if (CopyStatus.Failed == copyStatus && blob.CopyState.StatusDescription.Contains("Copy failed when reading the source"))
                            return BlobStatus.ShouldRetry;
                        return BlobStatus.Failed;
                    });
            }
            catch (Exception e)
            {
                var inner = e;
                while (inner.InnerException != null)
                    inner = inner.InnerException;

                // This catches when our shared access key has expired before requesting the copy to start
                var webException = inner as System.Net.WebException;
                var blobStatus = webException != null && webException.Status == WebExceptionStatus.ProtocolError ? BlobStatus.ShouldRetry : BlobStatus.Failed;
                return onSuccess(notStarted, () => blobStatus.ToTask());
            }
        }
    }
}
