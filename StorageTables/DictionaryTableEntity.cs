using BlackBarLabs.Persistence.Azure.StorageTables;
using EastFive.Collections.Generic;
using EastFive.Extensions;
using EastFive.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EastFive.Persistence.Azure.StorageTables
{
    public class DictionaryTableEntity<TValue> : ITableEntity
    {
        public string RowKey { get; set; }
        public string PartitionKey { get; set; }
        public DateTimeOffset Timestamp { get; set; }
        public string ETag { get; set; }
        public string TableName { get; set; }

        public IDictionary<string, TValue> values { get; set; }

        public DictionaryTableEntity()
        {

        }

        public DictionaryTableEntity(string rowKey, string partitionKey,
            IDictionary<string, TValue> values)
        {
            this.ETag = "*";
            this.RowKey = rowKey;
            this.PartitionKey = partitionKey;
            this.values = values;
        }

        public void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            this.values = properties
                .Select(kvp => kvp.Value.Bind(typeof(TValue),
                    obj => ((TValue)obj).PairWithKey(kvp.Key),
                    () => default(KeyValuePair<string, TValue>?)))
                .SelectWhereHasValue()
                .ToDictionary();
        }

        public IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            return values
                .Select(value => value.Value.CastEntityProperty(typeof(TValue),
                    ep => ep.PairWithKey(value.Key),
                    () => throw new Exception()))
                .ToDictionary();
        }
    }
}
