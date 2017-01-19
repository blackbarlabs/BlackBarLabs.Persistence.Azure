using BlackBarLabs.Persistence.Azure.StorageTables;
using System;
using System.Collections.Generic;
using System.IO;
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

        public static async Task<TResult> SaveBlobIfNotExistsAsync<TResult>(this Persistence.Azure.DataStores context, string containerReference, Guid id, byte[] data,
            Func<TResult> success,
            Func<string, TResult> failure)
        {
            try
            {
                var blockId = id.AsRowKey();
                var container = context.BlobStore.GetContainerReference(containerReference);
                if (!container.Exists())
                    return await context.SaveBlobAsync(containerReference, id, data, success, failure);

                var blockBlob = container.GetBlockBlobReference(blockId);
                if (!await blockBlob.ExistsAsync())
                    return await context.SaveBlobAsync(containerReference, id, data, success, failure);

                return success(); // The container and the blob existed, so nothing to do 
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

        public static async Task<TResult> ReadBlobAsync<TResult>(this Persistence.Azure.DataStores context, string containerReference, Guid id, 
            Func<Stream, TResult> success,
            Func<string, TResult> failure)
        {
            try
            {
                var container = context.BlobStore.GetContainerReference(containerReference);
                var blockId = id.AsRowKey();
                var blob = container.GetBlobReference(blockId);
                var returnStream = await blob.OpenReadAsync();
                return success(returnStream);
            }
            catch (Exception ex)
            {
                return failure(ex.Message);
            }

        }
    }
}
