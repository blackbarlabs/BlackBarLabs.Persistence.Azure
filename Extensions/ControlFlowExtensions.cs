using Microsoft.WindowsAzure.Storage.RetryPolicies;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BlackBarLabs.Persistence.Azure
{
    public static class ControlFlowExtensions
    {
        public delegate Task RetryDelegateAsync<T>(Action<T> onSuccess);
        public static async Task<T> RetryAsync<T>(this IRetryPolicy retryPolicy,
            RetryDelegateAsync<T> callback,
            Func<T> onFailureCallback,
            int retryHttpStatus = 200, Exception retryException = default(Exception))
        {
            if (default(Exception) == retryException)
                retryException = new Exception();

            var retriesAttempted = 0;
            TimeSpan retryDelay;
            var result = default(T);
            while (retryPolicy.ShouldRetry(retriesAttempted++, retryHttpStatus, retryException, out retryDelay, null))
            {
                var unlockSucceeded = false;
                await callback((updatedResult) =>
                {
                    unlockSucceeded = true;
                    result = updatedResult;
                });

                if (unlockSucceeded)
                    return result;

                await Task.Delay(retryDelay);
            }
            return onFailureCallback();
        }
    }
}
