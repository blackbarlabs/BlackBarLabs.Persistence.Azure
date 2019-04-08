using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EastFive.Persistence.Azure.StorageTables
{
    interface IProvideTable
    {
        CloudTable GetTable(Type type, CloudTableClient client);

        object GetTableQuery<TEntity>(string whereExpression = null);
    }
}
