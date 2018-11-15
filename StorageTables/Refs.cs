using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BlackBarLabs.Persistence.Azure;
using BlackBarLabs.Persistence.Azure.StorageTables;
using EastFive.Extensions;
using EastFive.Linq;
using EastFive.Linq.Async;
using EastFive.Persistence.Azure.StorageTables.Driver;
using Microsoft.WindowsAzure.Storage.Table;

namespace EastFive.Azure.Persistence
{
    public class Refs<TResource> : IRefs<TResource>
        where TResource : struct
    {
        public Refs(Guid [] ids)
        {
            this.ids = ids;
        }

        public Guid[] ids { get; private set; }

        private IEnumerableAsync<TResource> values;
        public IEnumerableAsync<TResource> Values
        {
            get
            {
                if(values.IsDefault())
                {
                    var driver = AzureTableDriverDynamic.FromSettings();
                    values = ids
                        .SelectAsyncOptional<Guid, TResource>(
                            (id, next, skip) =>
                            {
                                var rowKey = id.AsRowKey();
                                var partition = rowKey.GeneratePartitionKey();
                                return driver.FindByIdAsync(rowKey, partition,
                                    (TResource res) => next(res),
                                    () => skip());
                            });
                }
                return values;
            }
        }
    }
}
