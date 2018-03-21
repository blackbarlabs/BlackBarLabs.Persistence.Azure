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

    public struct IntervalCalculator
    {
        private int basis;
        private int cycles;
        private TimeSpan? nextInterval;

        public static readonly IntervalCalculator Default = new IntervalCalculator();

        public IntervalCalculator Concat(TimeSpan lastInterval, int[] data)
        {
            var zeroBasis = data.Count(x => x == 0);
            var dataCycles = data.Sum();
            var dataBasis = data.Length - zeroBasis;
            var lastSeconds = lastInterval.TotalSeconds;
            if (dataCycles != 0 && dataCycles == dataBasis)
                lastSeconds *= 0.9; // reduce by 10 % to allow performance to improve over time when the interval hasn't changed.
            return new IntervalCalculator
            {
                cycles = this.cycles + dataCycles,
                basis = this.basis + dataBasis,
                nextInterval = dataBasis > 0 ? TimeSpan.FromSeconds(dataCycles * lastSeconds / dataBasis) : this.nextInterval
            };
        }

        public IntervalCalculator Concat(IntervalCalculator calc)
        {
            return new IntervalCalculator
            {
                basis = this.basis + calc.basis,
                cycles = this.cycles + calc.cycles,
                nextInterval = calc.nextInterval
            };
        }

        public TimeSpan GetNextInterval(TimeSpan lower, TimeSpan higher)
        {
            // before data exists, default to the median of the range
            var result = nextInterval.HasValue ? nextInterval.Value : TimeSpan.FromSeconds((higher - lower).TotalSeconds / 2);
            if (result > higher)
                return higher;
            if (result < lower)
                return lower;
            return result;
        }
    }

    public struct ContainerStatistics
    {
        public IntervalCalculator calc;
        public string[] errors;
        public int successes;
        public KeyValuePair<CloudBlob, BlobStatus>[] retries;
        public KeyValuePair<CloudBlob, BlobStatus>[] failures;

        public static readonly ContainerStatistics Default = new ContainerStatistics
        {
            calc = IntervalCalculator.Default,
            errors = new string[] { },
            successes = 0,
            retries = new KeyValuePair<CloudBlob, BlobStatus>[] { },
            failures = new KeyValuePair<CloudBlob, BlobStatus>[] { }
        };

        public ContainerStatistics Concat(ContainerStatistics stats)
        {
            return new ContainerStatistics
            {
                calc = this.calc.Concat(stats.calc),
                errors = this.errors.Concat(stats.errors).ToArray(),
                successes = this.successes + stats.successes,
                retries = this.retries.Concat(stats.retries).ToArray(),
                failures = this.failures.Concat(stats.failures).ToArray()
            };
        }

        public ContainerStatistics Concat(string[] errors)
        {
            return new ContainerStatistics
            {
                calc = this.calc,
                errors = this.errors.Concat(errors).ToArray(),
                successes = this.successes,
                retries = this.retries,
                failures = this.failures
            };
        }
    }

    public struct BlobCopyOptions
    {
        public TimeSpan accessPeriod;
        public int maxConcurrency;
        public TimeSpan minIntervalCheckCopy;
        public TimeSpan maxIntervalCheckCopy;
        public TimeSpan maxTotalWaitForCopy;
        public int copyRetries;

        public static readonly BlobCopyOptions Default = new BlobCopyOptions
        {
            accessPeriod = TimeSpan.FromMinutes(60),
            maxConcurrency = 200,
            minIntervalCheckCopy = TimeSpan.FromSeconds(1),
            maxIntervalCheckCopy = TimeSpan.FromSeconds(15),
            maxTotalWaitForCopy = TimeSpan.FromMinutes(5),
            copyRetries = 5
        };
    }

    public static class Container
    {
        private struct BlobAccess
        {
            public string error;
            public string key;
            public DateTime expiresUtc;
        }

        private struct SparseCloudBlob
        {
            public string name;
            public string contentMD5;
            public long length;
        }

        private static readonly BlobRequestOptions RetryOptions =
            new BlobRequestOptions
            {
                ServerTimeout = TimeSpan.FromSeconds(90),
                RetryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(4), 10)
            };

        private static readonly AccessCondition EmptyCondition = AccessCondition.GenerateEmptyCondition();

        private static OperationContext CreateContext()
        {
            return new OperationContext();
        }

        public static async Task<TResult> FindAllContainersAsync<TResult>(this CloudBlobClient sourceClient, Func<CloudBlobContainer[], TResult> onSuccess, Func<string,TResult> onFailure)
        {
            var context = CreateContext();
            BlobContinuationToken token = null;
            var containers = new List<CloudBlobContainer>();
            while (true)
            {
                try
                {
                    var segment = await sourceClient.ListContainersSegmentedAsync(null, ContainerListingDetails.All, 
                        null, token, RetryOptions, context);
                    var results = segment.Results.ToArray();
                    containers.AddRange(results);
                    token = segment.ContinuationToken;
                    if (null == token)
                        return onSuccess(containers.ToArray());
                }
                catch (Exception e)
                {
                    return onFailure($"Exception listing all containers, Detail: {e.Message}");
                }
            }
        }

        public static async Task<KeyValuePair<string, ContainerStatistics>> CopyContainerAsync(this CloudBlobContainer sourceContainer, CloudBlobClient targetClient, DateTime whenStartedUtc, BlobCopyOptions copyOptions)
        {
            var targetContainerName = sourceContainer.Name;
            return await await sourceContainer.CreateOrUpdateTargetContainerForCopyAsync(targetClient, targetContainerName,
                async (targetContainer, findExistingAsync, renewAccessAsync, releaseAccessAsync) =>
                {
                    try
                    {
                        var existingTargetBlobs = await findExistingAsync();
                        var pair = default(BlobContinuationToken).PairWithValue(ContainerStatistics.Default.Concat(existingTargetBlobs.Key));
                        BlobAccess access = default(BlobAccess);
                        Func<Task<BlobAccess>> renewWhenExpiredAsync =
                            async () =>
                            {
                                if (access.expiresUtc < DateTime.UtcNow + TimeSpan.FromMinutes(5))
                                {
                                    access = await renewAccessAsync(copyOptions.accessPeriod);
                                    await Task.Delay(TimeSpan.FromSeconds(10));  // let settle in so first copy will be ok
                                }
                                return access;
                            };
                        while (true)
                        {
                            pair = await await sourceContainer.FindNextBlobSegmentAsync<Task<KeyValuePair<BlobContinuationToken,ContainerStatistics>>>(pair.Key,
                                async (token, blobs) =>
                                {
                                    var stats = await blobs.CopyBlobsWithContainerKeyAsync(targetContainer, existingTargetBlobs.Value, 
                                        () => pair.Value.calc.GetNextInterval(copyOptions.minIntervalCheckCopy, copyOptions.maxIntervalCheckCopy), 
                                        copyOptions.maxTotalWaitForCopy, renewWhenExpiredAsync, copyOptions.maxConcurrency);
                                    blobs = null;
                                    return token.PairWithValue(pair.Value.Concat(stats));
                                },
                                why => default(BlobContinuationToken).PairWithValue(pair.Value.Concat(new[] { why })).ToTask());
                            if (default(BlobContinuationToken) == pair.Key)
                            {
                                if (pair.Value.retries.Any())
                                {
                                    var copyRetries = copyOptions.copyRetries;
                                    existingTargetBlobs = await findExistingAsync();
                                    pair = default(BlobContinuationToken).PairWithValue(pair.Value.Concat(existingTargetBlobs.Key));
                                    var checkCopyCompleteAfter = pair.Value.calc.GetNextInterval(copyOptions.minIntervalCheckCopy, copyOptions.maxIntervalCheckCopy);
                                    while (copyRetries-- > 0)
                                    {
                                        var stats = await pair.Value.retries
                                            .Select(x => x.Key)
                                            .ToArray()
                                            .CopyBlobsWithContainerKeyAsync(targetContainer, existingTargetBlobs.Value, 
                                                () => pair.Value.calc.GetNextInterval(copyOptions.minIntervalCheckCopy, copyOptions.maxIntervalCheckCopy), 
                                                copyOptions.maxTotalWaitForCopy, renewWhenExpiredAsync, copyOptions.maxConcurrency);
                                        pair = default(BlobContinuationToken).PairWithValue(new ContainerStatistics
                                        {
                                            calc = pair.Value.calc.Concat(stats.calc),
                                            errors = pair.Value.errors.Concat(stats.errors).ToArray(),
                                            successes = pair.Value.successes + stats.successes,
                                            retries = stats.retries,
                                            failures = pair.Value.failures.Concat(stats.failures).ToArray()
                                        });
                                        if (!pair.Value.retries.Any())
                                            break;
                                    }
                                }
                                return sourceContainer.Name.PairWithValue(pair.Value);
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        return sourceContainer.Name.PairWithValue(ContainerStatistics.Default.Concat(new [] { $"Exception copying container, Detail: {e.Message}" }));
                    }
                    finally
                    {
                        await releaseAccessAsync();
                    }
                },
                why => sourceContainer.Name.PairWithValue(ContainerStatistics.Default.Concat(new[] { why })).ToTask());
        }

        private static async Task<TResult> CreateOrUpdateTargetContainerForCopyAsync<TResult>(this CloudBlobContainer sourceContainer,
           CloudBlobClient blobClient, string targetContainerName,
           Func<CloudBlobContainer, Func<Task<KeyValuePair<string[],SparseCloudBlob[]>>>, Func<TimeSpan, Task<BlobAccess>>, Func<Task>, TResult> onSuccess, Func<string,TResult> onFailure)
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
                var keyName = $"{sourceContainer.ServiceClient.Credentials.AccountName}-{targetContainerName}-access";
                return onSuccess(targetContainer,
                    () =>
                    {
                        return targetContainer.FindAllBlobsAsync(
                            blobs => new string[] { }.PairWithValue(blobs),
                            (why, partialBlobList) => new[] { why }.PairWithValue(partialBlobList));
                    },
                    async (sourceAccessWindow) =>
                    {
                        try
                        {
                            var renewContext = CreateContext();
                            var permissions = await sourceContainer.GetPermissionsAsync(EmptyCondition, RetryOptions, renewContext);
                            permissions.SharedAccessPolicies.Clear();
                            var access = new BlobAccess
                            {
                                key = keyName,
                                expiresUtc = DateTime.UtcNow.Add(sourceAccessWindow)
                            };
                            permissions.SharedAccessPolicies.Add(access.key, new SharedAccessBlobPolicy
                            {
                                SharedAccessExpiryTime = access.expiresUtc,
                                Permissions = SharedAccessBlobPermissions.Read
                            });
                            await sourceContainer.SetPermissionsAsync(permissions, EmptyCondition, RetryOptions, renewContext);
                            return access;
                        }
                        catch (Exception e)
                        {
                            return new BlobAccess { error = $"Error renewing access policy on container, Detail: {e.Message}" };
                        }
                    },
                    async () =>
                    {
                        try
                        {
                            var releaseContext = CreateContext();
                            var permissions = await sourceContainer.GetPermissionsAsync(EmptyCondition, RetryOptions, releaseContext);
                            permissions.SharedAccessPolicies.Clear();
                            await sourceContainer.SetPermissionsAsync(permissions, EmptyCondition, RetryOptions, releaseContext);
                        }
                        catch (Exception e)
                        {
                            // Failure here is no big deal as the container will still be usable
                            return;
                        }
                    });
            }
            catch (Exception e)
            {
                return onFailure($"Exception preparing container for copy, Detail: {e.Message}");
            }
        }

        private static async Task<TResult> FindAllBlobsAsync<TResult>(this CloudBlobContainer container, Func<SparseCloudBlob[], TResult> onSuccess, Func<string, SparseCloudBlob[], TResult> onFailure)
        {
            var context = CreateContext();
            BlobContinuationToken token = null;
            var blobs = new List<SparseCloudBlob>();
            while (true)
            {
                try
                {
                    var segment = await container.ListBlobsSegmentedAsync(null, true,
                        BlobListingDetails.UncommittedBlobs, null, token, RetryOptions, context);
                    var results = segment.Results.ToArray();
                    blobs.AddRange(results
                        .Cast<CloudBlob>()
                        .Select(x => new SparseCloudBlob
                        {
                            name = x.Name,
                            contentMD5 = x.Properties.ContentMD5,
                            length = x.Properties.Length
                        }));
                    token = segment.ContinuationToken;
                    if (null == token)
                        return onSuccess(blobs.ToArray());
                }
                catch (Exception e)
                {
                    return onFailure($"Exception listing all blobs, Detail: {e.Message}", blobs.ToArray());
                }
            }
        }

        private static async Task<TResult> FindNextBlobSegmentAsync<TResult>(this CloudBlobContainer container, BlobContinuationToken token, Func<BlobContinuationToken, CloudBlob[], TResult> onSuccess, Func<string,TResult> onFailure)
        {
            var context = CreateContext();
            try
            {
                var segment = await container.ListBlobsSegmentedAsync(null, true,
                    BlobListingDetails.UncommittedBlobs, null, token, RetryOptions, context);
                var results = segment.Results.Cast<CloudBlob>().ToArray();
                token = segment.ContinuationToken;
                return onSuccess(token, results);
            }
            catch (Exception e)
            {
                return onFailure($"Exception listing next blob segment, Detail: {e.Message}");
            }
        }

        private static async Task<ContainerStatistics> CopyBlobsWithContainerKeyAsync(this CloudBlob[] sourceBlobs, CloudBlobContainer targetContainer, SparseCloudBlob[] existingTargetBlobs, Func<TimeSpan> getCheckInterval, TimeSpan maxTotalWaitForCopy, Func<Task<BlobAccess>> renewAccessAsync, int maxConcurrency)
        {
            var access = await renewAccessAsync();
            if (!string.IsNullOrEmpty(access.error))
            {
                return ContainerStatistics.Default.Concat(new[] { access.error });
            }
            var checkInterval = getCheckInterval();
            var items = await sourceBlobs
                .Select(blob => blob.StartCopyAndWaitForCompletionAsync(targetContainer, access.key, existingTargetBlobs, checkInterval, maxTotalWaitForCopy))
                .WhenAllAsync(maxConcurrency);

            return new ContainerStatistics
            {
                calc = IntervalCalculator.Default.Concat(checkInterval, items.Select(x => x.Item3).ToArray()),
                errors = new string[] { },
                successes = items.Count(item => item.Item2 == BlobStatus.CopySuccessful),
                retries = items.Where(item => item.Item2 == BlobStatus.ShouldRetry).Select(item => item.Item1.PairWithValue(item.Item2)).ToArray(),
                failures = items.Where(item => item.Item2 == BlobStatus.Failed).Select(item => item.Item1.PairWithValue(item.Item2)).ToArray()
            };
        }

        private static async Task<Tuple<CloudBlob,BlobStatus,int>> StartCopyAndWaitForCompletionAsync(this CloudBlob sourceBlob, CloudBlobContainer targetContainer, string accessKey, SparseCloudBlob[] existingTargetBlobs, TimeSpan checkInterval, TimeSpan maxTotalWaitForCopy)
        {
            return await await sourceBlob.StartBackgroundCopyAsync(targetContainer, accessKey, existingTargetBlobs,
                async (started, progressAsync) =>
                {
                    try
                    {
                        var waitUntil = DateTime.UtcNow + maxTotalWaitForCopy;
                        var cycles = 0;
                        while (true)
                        {
                            if (started)
                            {
                                if (waitUntil < DateTime.UtcNow)
                                    return new Tuple<CloudBlob,BlobStatus,int>(sourceBlob,BlobStatus.ShouldRetry,cycles);
                                await Task.Delay(checkInterval);
                                cycles++;
                            }
                            var status = await progressAsync();
                            if (BlobStatus.Running == status)
                                continue;
                            return new Tuple<CloudBlob, BlobStatus, int>(sourceBlob, status, cycles);
                        }
                    }
                    catch (Exception e)
                    {
                        var inner = e;
                        while (inner.InnerException != null)
                            inner = inner.InnerException;

                        // This catches when our shared access key has expired after the copy has begun
                        var status = inner.Message.Contains("could not finish the operation within specified timeout") ? BlobStatus.ShouldRetry : BlobStatus.Failed;
                        return new Tuple<CloudBlob, BlobStatus, int>(sourceBlob, status, 3); // 3 is arbitrary here just to make these failures more weighty
                    }
                });
        }

        private static async Task<TResult> StartBackgroundCopyAsync<TResult>(this CloudBlob sourceBlob, CloudBlobContainer targetContainer, string accessKey, SparseCloudBlob[] existingTargetBlobs,
            Func<bool, Func<Task<BlobStatus>>, TResult> onSuccess) // started, progressAsync
        {
            var started = true;
            var notStarted = !started;
            var existingTarget = existingTargetBlobs.FirstOrDefault(tb => tb.name == sourceBlob.Name);
            if (!string.IsNullOrEmpty(existingTarget.name) && 
                existingTarget.contentMD5 == sourceBlob.Properties.ContentMD5 && 
                existingTarget.length == sourceBlob.Properties.Length)
                return onSuccess(notStarted, () => BlobStatus.CopySuccessful.ToTask());

            var target = targetContainer.GetReference(sourceBlob.BlobType, sourceBlob.Name);
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
                var blobStatus = (webException != null && webException.Status == WebExceptionStatus.ProtocolError) || 
                    inner.Message.Contains("could not finish the operation within specified timeout") ? BlobStatus.ShouldRetry : BlobStatus.Failed;
                return onSuccess(notStarted, () => blobStatus.ToTask());
            }
        }
    }
}
