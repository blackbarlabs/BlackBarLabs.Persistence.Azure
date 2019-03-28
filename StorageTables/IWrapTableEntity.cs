using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EastFive.Persistence.Azure.StorageTables
{
    public interface IWrapTableEntity<TEntity> : ITableEntity
    {

        TEntity Entity { get; }


        // CloudTable GetTable(CloudTableClient tableClient);
    }
}
