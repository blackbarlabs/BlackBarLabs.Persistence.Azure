using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EastFive.Azure.StorageTables.Driver
{
    public abstract partial class AzureStorageDriver
    {
        #region Generic delegates


        protected const int DefaultNumberOfTimesToRetry = 10;
        protected static readonly TimeSpan DefaultBackoffForRetry = TimeSpan.FromSeconds(4);
        protected readonly ExponentialRetry retryPolicy = new ExponentialRetry(DefaultBackoffForRetry, DefaultNumberOfTimesToRetry);

        public delegate Task SaveDocumentDelegate<TDocument>(TDocument documentInSavedState);
        public delegate Task RetryDelegate(int statusCode, Exception ex, Func<Task> retry);
        public delegate Task<TResult> RetryDelegateAsync<TResult>(
            Func<TResult> retry,
            Func<int, TResult> timeout);



        #region Utility methods

        internal static RetryDelegate GetRetryDelegate()
        {
            var retriesAttempted = 0;
            var retryPolicy = new Microsoft.WindowsAzure.Storage.RetryPolicies.ExponentialRetry(DefaultBackoffForRetry, DefaultNumberOfTimesToRetry);
            return async (statusCode, ex, retry) =>
            {
                TimeSpan retryDelay;
                bool shouldRetry = retryPolicy.ShouldRetry(retriesAttempted++, statusCode, ex, out retryDelay, null);
                if (!shouldRetry)
                    throw new Exception("After " + retriesAttempted + "attempts finding the resource timed out");
                await Task.Delay(retryDelay);
                await retry();
            };
        }

        public static RetryDelegateAsync<TResult> GetRetryDelegateContentionAsync<TResult>(
            int maxRetries = 100)
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

        protected static RetryDelegateAsync<TResult> GetRetryDelegateCollisionAsync<TResult>(
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

        #endregion

        #endregion

        public abstract Task<TResult> UpdateIfNotModifiedAsync<TData, TResult>(TData data,
            Func<TResult> success,
            Func<TResult> documentModified,
            Func<ExtendedErrorInformationCodes, string, TResult> onFailure =
                default(Func<ExtendedErrorInformationCodes, string, TResult>),
            RetryDelegate onTimeout =
                default(RetryDelegate))
            where TData : ITableEntity;

        public abstract Task<TResult> DeleteAsync<TData, TResult>(TData document,
            Func<TResult> success,
            Func<TResult> onNotFound,
            Func<ExtendedErrorInformationCodes, string, TResult> onFailure =
                default(Func<ExtendedErrorInformationCodes, string, TResult>),
            RetryDelegate onTimeout = default(RetryDelegate))
            where TData : class, ITableEntity;

        public abstract Task<TResult> CreateAsync<TResult, TDocument>(string rowKey, string partitionKey, TDocument document,
           Func<TResult> onSuccess,
           Func<TResult> onAlreadyExists,
            Func<ExtendedErrorInformationCodes, string, TResult> onFailure =
                default(Func<ExtendedErrorInformationCodes, string, TResult>),
           RetryDelegate onTimeout = 
                default(RetryDelegate))
           where TDocument : class, ITableEntity;

        public abstract Task<TResult> FindByIdAsync<TEntity, TResult>(
                string rowKey, string partitionKey,
            Func<TEntity, TResult> onSuccess, 
            Func<TResult> onNotFound,
            Func<ExtendedErrorInformationCodes, string, TResult> onFailure =
                default(Func<ExtendedErrorInformationCodes, string, TResult>),
            RetryDelegate onTimeout =
                default(RetryDelegate))
            where TEntity : class, ITableEntity;
    }
}
