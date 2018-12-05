using System;
using System.Linq;
using EastFive.Extensions;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace EastFive.Azure.StorageTables.Driver
{
    /// <summary>
    /// An enumeration of the errors that can be seen from using the AST driver in C#.
    /// The full list of errors is available here: https://docs.microsoft.com/en-us/rest/api/storageservices/table-service-error-codes
    /// </summary>
    public enum ExtendedErrorInformationCodes
    {   
        /// <summary>
        /// Bad Request (400): A property is specified more than one time.
        /// </summary>
        DuplicatePropertiesSpecified,

        /// <summary>
        /// Conflict (409): The specified entity already exists.
        /// </summary>
        EntityAlreadyExists,

        /// <summary>
        /// Bad Request (400): The entity is larger than the maximum size permitted.
        /// </summary>
        EntityTooLarge,

        /// <summary>
        /// Bad Request (400): The value specified is invalid.
        /// </summary>
        InvalidValueType,

        /// <summary>
        ///  Bad Request (400): Values have not been specified for all properties in the entity.
        /// </summary>
        PropertiesNeedValue,

        /// <summary>
        /// Bad Request (400): The property name is invalid.
        /// </summary>
        PropertyNameInvalid,

        /// <summary>
        /// Bad Request (400): The property name exceeds the maximum allowed length.
        /// </summary>
        PropertyNameTooLong,

        /// <summary>
        /// Bad Request (400): The property value is larger than the maximum size permitted.
        /// </summary>
        PropertyValueTooLarge,

        /// <summary>
        /// Conflict (409): The table specified already exists.
        /// </summary>
        TableAlreadyExists,

        /// <summary>
        /// Conflict (409): The specified table is being deleted.
        /// </summary>
        TableBeingDeleted,

        /// <summary>
        /// Not Found (404): The table specified does not exist.
        /// </summary>
        TableNotFound,

        /// <summary>
        /// Bad Request (400): The entity contains more properties than allowed.
        /// </summary>
        TooManyProperties,

        /// <summary>
        /// Precondition Failed (412): The update condition specified in the request was not satisfied.
        /// </summary>
        UpdateConditionNotSatisfied,

        /// <summary>
        /// (408): There was a timeout while connecting to AST.
        /// </summary>
        Timeout,

        /// <summary>
        /// Error code that is not covered by this enum.
        /// </summary>
        Other,

        /// <summary>
        /// Error condition that was not specifed by extended information
        /// </summary>
        General,
    }

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

        public static bool IsProblemPropertyValueTooLarge(this StorageException exception)
        {
            return exception.ParseExtendedErrorInformation(
                (errorCode, errorMessage) => errorCode == ExtendedErrorInformationCodes.PropertyValueTooLarge,
                () => false);
        }
        
        public static bool IsProblemTimeout(this StorageException exception)
        {
            if (exception.InnerException is System.Net.WebException)
            {
                var webEx = (System.Net.WebException)exception.InnerException;
                return (webEx.Status == System.Net.WebExceptionStatus.Timeout);
            }
            if (408 == exception.RequestInformation.HttpStatusCode)
                return true;

            return false;
        }

        public static bool IsProblemResourceAlreadyExists(this StorageException exception)
        {
            return exception.ParseExtendedErrorInformation(
                (errorCode, errorMessage) => errorCode == ExtendedErrorInformationCodes.EntityAlreadyExists,
                () => false);
        }

        public static bool IsProblemTableDoesNotExist(this StorageException exception)
        {
            if (exception.RequestInformation.ExtendedErrorInformation.ErrorCode == "TableNotFound")
                return true;
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

        public static bool IsProblemDoesNotExist(this StorageException exception)
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

        public static TResult ParseStorageException<TResult>(this StorageException storageException,
            Func<ExtendedErrorInformationCodes, string, TResult> onParsed,
            Func<TResult> onUnableToParse)
        {
            return storageException.ParseExtendedErrorInformation(
                onParsed,
                () =>
                {
                    if (storageException.IsProblemTimeout())
                        return onParsed(ExtendedErrorInformationCodes.Timeout, "The request to AST timed out.");

                    if (storageException.InnerException is System.Net.WebException)
                    {
                        var webEx = storageException.InnerException as System.Net.WebException;
                        if (webEx.Response is System.Net.HttpWebResponse)
                        {
                            var httpWebResponse = webEx.Response as System.Net.HttpWebResponse;
                            try
                            {
                                var responseContent = httpWebResponse.GetResponseStream().ReadAsString();
                                System.Diagnostics.Debug.WriteLine($"AST replied:{responseContent}");
                                return onParsed(ExtendedErrorInformationCodes.General, responseContent);
                            }
                            catch (Exception exFromStream)
                            {
                                exFromStream.GetType(); // Suppress warning
                                return onUnableToParse();
                            }
                        }
                    }
                    return onUnableToParse();
                });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="storageException"></param>
        /// <param name="onExtendedErrorInformation">Invoked with the RequestInformation.ExtendedErrorInformation. (ErrorCode and ErrorMessage, respectively) from the exception.</param>
        /// <param name="onExtendedErrorInformationNotProvided">Invoked when extended error information is not available</param>
        /// <returns></returns>
        public static TResult ParseExtendedErrorInformation<TResult>(this StorageException storageException,
            Func<ExtendedErrorInformationCodes, string, TResult> onExtendedErrorInformation,
            Func<TResult> onExtendedErrorInformationNotProvided)
        {
            if (storageException.RequestInformation.IsDefaultOrNull())
                return onExtendedErrorInformationNotProvided();
            if (storageException.RequestInformation.ExtendedErrorInformation.IsDefaultOrNull())
                return onExtendedErrorInformationNotProvided();

            var errorCode = storageException.RequestInformation.ExtendedErrorInformation.ErrorCode;
            var errorMessage = storageException.RequestInformation.ExtendedErrorInformation.ErrorMessage;
            System.Diagnostics.Debug.WriteLine($"AST Error [{errorCode}]:{errorMessage}");

            var matchingCodes = Enum.GetNames(typeof(ExtendedErrorInformationCodes))
                .Where(errorCodeName => String.Compare(errorCodeName, errorCode) == 0);
            
            if (!matchingCodes.Any())
                return onExtendedErrorInformation(ExtendedErrorInformationCodes.Other, errorMessage);

            var matchingCodeString = matchingCodes.First();
            Enum.TryParse(matchingCodeString, out ExtendedErrorInformationCodes matchingCode);
            return onExtendedErrorInformation(matchingCode, errorMessage);
        }
    }
}
