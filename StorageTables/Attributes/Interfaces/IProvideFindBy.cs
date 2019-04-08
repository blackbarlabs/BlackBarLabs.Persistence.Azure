using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using EastFive.Linq.Async;

namespace EastFive.Persistence.Azure.StorageTables
{
    public interface IProvideFindBy
    {
        IEnumerableAsync<KeyValuePair<string, string>> GetKeys<TEntity>(IRef<TEntity> value, Driver.AzureTableDriverDynamic repository)
            where TEntity : struct, IReferenceable;
    }
}
