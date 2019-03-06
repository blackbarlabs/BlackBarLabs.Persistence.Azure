using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EastFive.Persistence.Azure.StorageTables
{
    interface IProvideEntity
    {
        ITableEntity GetEntity<TEntity>(TEntity entity, string etag = "*");

        TEntity CreateEntityInstance<TEntity>(string rowKey, string partitionKey, 
            IDictionary<string, EntityProperty> properties,
            string etag, DateTimeOffset lastUpdated);
    }
}
