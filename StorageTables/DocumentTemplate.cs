using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace BlackBarLabs.Persistence.Azure.StorageTables
{
    [Serializable]
    public abstract class DocumentTemplate<TKey> : TableEntity, IDocument
    {
        protected DocumentTemplate()
        {
        }

        protected DocumentTemplate(TKey id)
        {
            RowKey = id.ToString().Replace("-", "");
            PartitionKey = RowKey.GeneratePartitionKey();
        }

        public int EntityState { get; set; }

    }
}
