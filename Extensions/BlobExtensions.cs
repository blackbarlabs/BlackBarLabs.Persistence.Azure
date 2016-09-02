using BlackBarLabs.Persistence.Azure.StorageTables;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BlackBarLabs.Identity.AzureStorageTables.Extensions
{
    public static class BlobExtensions
    {
        public static async Task<TResult> SaveBlobAsync<TResult>(this Persistence.Azure.DataStores context, string containerReference, Guid id, byte[] data,
            Func<TResult> success,
            Func<string, TResult> failure)
        {
            try
            {
                var blockId = id.AsRowKey();
                var container = context.BlobStore.GetContainerReference(containerReference);
                container.CreateIfNotExists();
                var blockBlob = container.GetBlockBlobReference(blockId);
                blockBlob.Metadata["id"] = blockId; // TODO: As row key
                await blockBlob.UploadFromByteArrayAsync(data, 0, data.Length);
                blockBlob.Properties.ContentType = "application/pdf";
                blockBlob.SetProperties();
                return success();
            }
            catch (Exception ex)
            {
                return failure(ex.Message);
            }
        }

        public static Task<TResult> SaveBlobAsync<TResult>(this Persistence.Azure.DataStores context, Type containerReference, Guid id, byte[] data,
            Func<TResult> success,
            Func<string, TResult> failure)
        {
            return context.SaveBlobAsync(containerReference.GetType().Name, id, data, success, failure);
        }
    }
}
