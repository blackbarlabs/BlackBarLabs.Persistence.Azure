using BlackBarLabs.Persistence.Azure;
using BlackBarLabs.Persistence.Azure.StorageTables;
using EastFive.Extensions;
using EastFive.Persistence.Azure.StorageTables.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EastFive.Azure.Persistence
{
    public class Ref<T> : EastFive.IRef<T>
        where T : struct
    {
        public Ref(Guid id)
        {
            this.id = id;
        }

        public T? value
        {
            get;
            private set;
        }

        public Guid id
        {
            get;
            private set;
        }

        public bool resolved
        {
            get;
            private set;
        }

        public async Task ResolveAsync()
        {
            var driver = AzureTableDriverDynamic.FromSettings();
            var rowKey = this.id.AsRowKey();
            this.resolved = await driver.FindByIdAsync(rowKey, rowKey.GeneratePartitionKey(),
                (T entity) =>
                {
                    this.value = entity;
                    return true;
                },
                () =>
                {
                    return false;
                });
        }
    }
}
