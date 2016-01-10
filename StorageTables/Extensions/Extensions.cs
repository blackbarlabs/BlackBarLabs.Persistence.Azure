using Microsoft.WindowsAzure.Storage;

namespace BlackBarLabs.Persistence.Azure.StorageTables
{
    public static class Extensions
    {
       
        
        public static bool IsProblemPreconditionFailed(this StorageException exception)
        {
            if (exception.InnerException is System.Net.WebException)
            {
                var webEx = (System.Net.WebException)exception.InnerException;
                if(webEx.Response is System.Net.HttpWebResponse)
                {
                    var httpResponse = (System.Net.HttpWebResponse)webEx.Response;
                    return (httpResponse.StatusCode == System.Net.HttpStatusCode.PreconditionFailed);
                }
            }
            return false;
        }

        public static bool IsProblemTimeout(this StorageException exception)
        {
            if (exception.InnerException is System.Net.WebException)
            {
                var webEx = (System.Net.WebException)exception.InnerException;
                return (webEx.Status == System.Net.WebExceptionStatus.Timeout);
            }
            return false;
        }

        public static bool IsProblemResourceAlreadyExists(this StorageException exception)
        {
            if (exception.InnerException is System.Net.WebException)
            {
                var webEx = (System.Net.WebException)exception.InnerException;

                if (webEx.Response is System.Net.HttpWebResponse)
                {
                    var httpResponse = (System.Net.HttpWebResponse)webEx.Response;
                    return (httpResponse.StatusCode == System.Net.HttpStatusCode.Conflict);
                }
            }
            return false;
        }
        public static bool IsProblemTableDoesNotExist(this StorageException exception)
        {
            if (exception.InnerException is System.Net.WebException)
            {
                var webEx = (System.Net.WebException)exception.InnerException;

                if (webEx.Response is System.Net.HttpWebResponse)
                {
                    var httpResponse = (System.Net.HttpWebResponse)webEx.Response;
                    return (httpResponse.StatusCode == System.Net.HttpStatusCode.NotFound);
                }
            }
            return false;
        }
        
        public static bool TranslateException(this StorageException exception)
        {
            if (exception.IsProblemResourceAlreadyExists())
                throw new StorageException();

            return false;
        }
    }
}
