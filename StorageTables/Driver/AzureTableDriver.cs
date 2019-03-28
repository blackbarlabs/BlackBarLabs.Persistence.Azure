using EastFive.Azure.StorageTables.Driver;
using EastFive.Extensions;
using EastFive.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace EastFive.Persistence.Azure.StorageTables.Driver
{
    public class AzureTableDriver
    {
        private string accountName;
        private string accountKey;

        public static AzureTableDriver FromSettings(string settingKey = EastFive.Azure.Persistence.AppSettings.Storage)
        {
            return EastFive.Web.Configuration.Settings.GetString(settingKey,
                (storageString) => FromStorageString(storageString),
                (why) => throw new Exception(why));
        }

        // DefaultEndpointsProtocol=https;AccountName=accountName;AccountKey=9jpXXXzm6CJSg==
        public static AzureTableDriver FromStorageString(string storageString)
        {
            return storageString.MatchRegexInvoke(".*;AccountName=(?<accountName>[a-zA-Z0-9]+);AccountKey=(?<accountKey>[a-zA-Z0-9\\-\\+=\\/]+)",
                (accountName, accountKey) => new AzureTableDriver(accountName, accountKey),
                (AzureTableDriver [] azureTableDrivers) =>
                {
                    return azureTableDrivers.First<AzureTableDriver, AzureTableDriver>(
                        (driver, next) => driver,
                        () => throw new Exception("Could not parse account string"));
                });
        }

        // humagelorderowltest2
        public AzureTableDriver(string accountName, string accountKey)
        {
            this.accountName = accountName;
            this.accountKey = accountKey;
        }
        
        public async Task<TResult> FindByIdAsync<TEntity, TResult>(
                string rowKey, string partitionKey,
            Func<TEntity, TResult> onSuccess,
            Func<TResult> onNotFound,
            Func<ExtendedErrorInformationCodes, string, TResult> onFailure =
                default(Func<ExtendedErrorInformationCodes, string, TResult>),
            AzureStorageDriver.RetryDelegate onTimeout =
                default(AzureStorageDriver.RetryDelegate))
        {
            var tableName = typeof(TEntity).GetAttributesInterface<IProvideTable>()
                .First(
                    (attr, next) => attr.TableName,
                    () => typeof(TEntity).Name);
            var propertyNames = typeof(TEntity)
                .GetProperties()
                .Where(propInfo => propInfo.ContainsAttributeInterface<IPersistInAzureStorageTables>())
                .Select(propInfo => propInfo.GetAttributesInterface<IPersistInAzureStorageTables>().First().GetTablePropertyName(propInfo))
                .Join(",");

            var http = new HttpClient(
                new SharedKeySignatureStoreTablesMessageHandler(this.accountName, this.accountKey))
            {
                Timeout = new TimeSpan(0, 5, 0)
            };
            
            var url = $"https://{accountName}.table.core.windows.net/{tableName}(PartitionKey='{partitionKey}',RowKey='{rowKey}')?$select=propertyNames";
            var response = await http.GetAsync(url);

            return onSuccess(default(TEntity));
        }

        public async Task<TResult> FindAllAsync<TEntity, TResult>(
                string filter,
            Func<TEntity[], TResult> onSuccess,
            Func<ExtendedErrorInformationCodes, string, TResult> onFailure =
                default(Func<ExtendedErrorInformationCodes, string, TResult>),
            AzureStorageDriver.RetryDelegate onTimeout =
                default(AzureStorageDriver.RetryDelegate))
        {
            var tableName = typeof(TEntity).GetAttributesInterface<IProvideTable>()
                .First(
                    (attr, next) => attr.TableName,
                    () => typeof(TEntity).Name);
            var propertyNames = typeof(TEntity)
                .GetProperties()
                .Where(propInfo => propInfo.ContainsAttributeInterface<IPersistInAzureStorageTables>())
                .Select(propInfo => propInfo.GetAttributesInterface<IPersistInAzureStorageTables>().First().GetTablePropertyName(propInfo))
                .Join(",");

            var http = new HttpClient(
                new SharedKeySignatureStoreTablesMessageHandler(this.accountName, this.accountKey))
            {
                Timeout = new TimeSpan(0, 5, 0)
            };

            var filterAndParameter = filter.IsNullOrWhiteSpace(
                () => string.Empty,
                queryExpression => $"$filter={queryExpression}&");
            var url = $"https://{accountName}.table.core.windows.net/{tableName}()?{filterAndParameter}$select=propertyNames";
            // https://myaccount.table.core.windows.net/mytable()?$filter=<query-expression>&$select=<comma-separated-property-names>
            var response = await http.GetAsync(url);

            return onSuccess(default(TEntity).AsArray());
        }

        
    }
}
