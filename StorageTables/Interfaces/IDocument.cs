using Microsoft.WindowsAzure.Storage.Table;

namespace BlackBarLabs.Persistence.Azure.StorageTables
{
    public interface IDocument : ITableEntity
    {
        int EntityState { get; set; }
    }
}
