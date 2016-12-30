using System;
using System.Linq;
using System.Threading.Tasks;

using Microsoft.WindowsAzure.Storage.Table;

using BlackBarLabs.Core.Extensions;
using BlackBarLabs.Persistence.Azure.StorageTables;
using BlackBarLabs.Core;

namespace BlackBarLabs.Persistence
{
    public static class DocumentExtensions
    {
        public static async Task<TResult> FindLinkedDocumentsAsync<TParentDoc, TLinkedDoc, TResult>(this AzureStorageRepository repo, 
            Guid parentDocId,
            Func<TParentDoc, Guid[]> getLinkedIds,
            Func<TParentDoc, TLinkedDoc[], TResult> found,
            Func<TResult> parentDocNotFound)
            where TParentDoc : class, ITableEntity
            where TLinkedDoc : class, ITableEntity
        {
            var result = await await repo.FindByIdAsync(parentDocId,
                async (TParentDoc document) =>
                {
                    var linkedDocIds = getLinkedIds(document);
                    var linkedDocsWithNulls = await linkedDocIds
                        .Select(
                            linkedDocId =>
                            {
                                return repo.FindByIdAsync(linkedDocId,
                                    (TLinkedDoc priceSheetDocument) => priceSheetDocument,
                                    () =>
                                    {
                                        // TODO: Log data corruption
                                        return default(TLinkedDoc);
                                    });
                            })
                        .WhenAllAsync();
                    var linkedDocs = linkedDocsWithNulls
                        .Where(doc => default(TLinkedDoc) != doc)
                        .ToArray();
                    return found(document, linkedDocs);
                },
               () => parentDocNotFound().ToTask());

            return result;
        }

        public static Guid? RemoveLinkedDocument<TJoin>(this TJoin[] joins, Guid joinId,
            Func<TJoin, Guid> idField,
            Func<TJoin, Guid> joinField,
            Action<TJoin[]> save)
        {
            var joinsUpdated = joins
                .Where(join => joinField(join) != joinId)
                .ToArray();
            save(joinsUpdated);
            var match = joins.Where(join => joinField(join) == joinId).ToArray();
            if (match.Length > 0)
                return idField(match[0]);
            return default(Guid?);
        }
    }
}
