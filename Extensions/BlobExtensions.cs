using BlackBarLabs.Persistence.Azure.StorageTables;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

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
                blockBlob.Properties.ContentType = "application/excel";
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
            Func<TResult> blobAlreadyExists,
            Func<string, TResult> failure)
        {
            try
            {
                var blockId = id.AsRowKey();
                var container = context.BlobStore.GetContainerReference(containerReference);
                if (!container.Exists())
                    return await context.SaveBlobAsync(containerReference, id, data, success, failure);

                var blockBlob = container.GetBlockBlobReference(blockId);
                if (blockBlob.Exists())
                    return blobAlreadyExists();

                return await context.SaveBlobAsync(containerReference, id, data, success, failure);
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

        public static async Task<TResult> DeleteBlobAsync<TResult>(this Persistence.Azure.DataStores context, string containerReference, Guid id,
            Func<TResult> success,
            Func<string, TResult> failure)
        {
            try
            {
                var container = context.BlobStore.GetContainerReference(containerReference);
                var blockId = id.AsRowKey();
                var blob = container.GetBlobReference(blockId);
                await blob.DeleteIfExistsAsync();
                return success();
            }
            catch (Exception ex)
            {
                return failure(ex.Message);
            }

        }

        public static async Task DeleteBlobContainerAsync(this Persistence.Azure.DataStores context, string containerReference)
        {
            var container = context.BlobStore.GetContainerReference(containerReference);
            if (container.Exists())
                await container.DeleteAsync();
        }

    }
}
