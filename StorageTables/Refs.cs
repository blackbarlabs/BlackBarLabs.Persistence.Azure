using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BlackBarLabs.Persistence.Azure.StorageTables;
using EastFive.Extensions;
using EastFive.Linq;
using EastFive.Linq.Async;
using Microsoft.WindowsAzure.Storage.Table;

namespace EastFive.Persistence.Azure.StorageTables
{
    public class Refs<TResource, TDocument> : IRefs<TResource>
        where TDocument : TableEntity
    {
        private AzureStorageRepository storageRepository;
        private Func<TDocument, TResource> convert;

        public Refs(Guid [] ids, AzureStorageRepository storageRepository)
        {
            this.ids = ids;
            this.storageRepository = storageRepository;
        }

        public Guid[] ids { get; private set; }

        private IEnumerableAsync<TResource> values;
        public IEnumerableAsync<TResource> Values
        {
            get
            {
                if(values.IsDefault())
                {
                    values = ids
                        .SelectAsyncOptional<Guid, TResource>(
                            (id, next, skip) => storageRepository.FindByIdAsync(id,
                                (TDocument res) => next(convert(res)),
                                () => skip()));
                }
                return values;
            }
        }
    }
}
