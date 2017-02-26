using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using BlackBarLabs.Persistence.Azure.StorageTables;
using BlackBarLabs.Extensions;
using System.Collections.Generic;
using BlackBarLabs.Collections.Generic;
using BlackBarLabs.Linq;
using BlackBarLabs.Linq.Async;

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

        public static async Task<TResult> FindLinkedDocumentAsync<TParentDoc, TLinkedDoc, TResult>(this AzureStorageRepository repo,
            Guid parentDocId,
            Func<TParentDoc, Guid> getLinkedId,
            Func<TParentDoc, TLinkedDoc, TResult> found,
            Func<TResult> parentDocNotFound)
            where TParentDoc : class, ITableEntity
            where TLinkedDoc : class, ITableEntity
        {
            var result = await await repo.FindByIdAsync(parentDocId,
                async (TParentDoc document) =>
                {
                    var linkedDocId = getLinkedId(document);
                    var linkedDoc = await repo.FindByIdAsync(linkedDocId,
                        (TLinkedDoc priceSheetDocument) => priceSheetDocument,
                        () =>
                        {
                            // TODO: Log data corruption
                            return default(TLinkedDoc);
                        });
                    return found(document, linkedDoc);
                },
               () => parentDocNotFound().ToTask());

            return result;
        }

        public static async Task<TResult> FindLinkedLinkedDocumentsAsync<TParentDoc, TMiddleDoc, TLinkedDoc, TResult>(this AzureStorageRepository repo,
            Guid parentDocId,
            Func<TParentDoc, Guid[]> getMiddleDocumentIds,
            Func<TMiddleDoc, Guid[]> getLinkedIds,
            Func<TParentDoc, IDictionary<TMiddleDoc, TLinkedDoc[]>, TResult> found,
            Func<TResult> lookupDocNotFound)
            where TParentDoc : class, ITableEntity
            where TMiddleDoc : class, ITableEntity
            where TLinkedDoc : class, ITableEntity
        {
            var result = await await repo.FindByIdAsync(parentDocId,
                async (TParentDoc parentDoc) =>
                {
                    var middleDocIds = getMiddleDocumentIds(parentDoc);
                    var middleAndLinkedDocs = await middleDocIds
                        .Select(
                            middleDocId =>
                                repo.FindLinkedDocumentsAsync(middleDocId,
                                    (middleDoc) => getLinkedIds(middleDoc),
                                    (TMiddleDoc middleDoc, TLinkedDoc[] linkedDocsByMiddleDoc) => 
                                        new KeyValuePair<TMiddleDoc, TLinkedDoc[]>(middleDoc, linkedDocsByMiddleDoc),
                                    () => default(KeyValuePair<TMiddleDoc, TLinkedDoc[]>?)))
                        .WhenAllAsync()
                        .SelectWhereHasValueAsync();
                    return found(parentDoc, middleAndLinkedDocs.ToDictionary());
                },
                () =>
                {
                    // TODO: Log data inconsistency here
                    return lookupDocNotFound().ToTask();
                });
            return result;
        }

        public static async Task<TResult> FindLinkedLinkedDocumentsAsync<TParentDoc, TMiddleDoc, TLinkedDoc, TResult>(this AzureStorageRepository repo,
            Guid parentDocId,
            Func<TParentDoc, Guid[]> getMiddleDocumentIds,
            Func<TMiddleDoc, Guid> getLinkedId,
            Func<TParentDoc, IDictionary<TMiddleDoc, TLinkedDoc>, TResult> found,
            Func<TResult> lookupDocNotFound)
            where TParentDoc : class, ITableEntity
            where TMiddleDoc : class, ITableEntity
            where TLinkedDoc : class, ITableEntity
        {
            var result = await await repo.FindByIdAsync(parentDocId,
                async (TParentDoc parentDoc) =>
                {
                    var middleDocIds = getMiddleDocumentIds(parentDoc);
                    var middleAndLinkedDocs = await middleDocIds
                        .Select(
                            middleDocId =>
                                repo.FindLinkedDocumentAsync(middleDocId,
                                    (middleDoc) => getLinkedId(middleDoc),
                                    (TMiddleDoc middleDoc, TLinkedDoc linkedDocsByMiddleDoc) =>
                                        new KeyValuePair<TMiddleDoc, TLinkedDoc>(middleDoc, linkedDocsByMiddleDoc),
                                    () => default(KeyValuePair<TMiddleDoc, TLinkedDoc>)))
                        .WhenAllAsync();
                    return found(parentDoc, middleAndLinkedDocs.ToDictionary());
                },
                () =>
                {
                    // TODO: Log data inconsistency here
                    return lookupDocNotFound().ToTask();
                });
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

        public static async Task<TResult> FindRecursiveDocumentsAsync<TDoc, TResult>(this AzureStorageRepository repo,
            Guid startingDocumentId,
            Func<TDoc, Guid?> getLinkedId,
            Func<TDoc[], TResult> onFound,
            Func<TResult> startDocNotFound)
            where TDoc : class, ITableEntity
        {
            var result = await await repo.FindByIdAsync(startingDocumentId,
                async (TDoc document) =>
                {
                    var linkedDocId = getLinkedId(document);
                    if (!linkedDocId.HasValue)
                        return onFound(document.ToEnumerable().ToArray());
                    return await repo.FindRecursiveDocumentsAsync(linkedDocId.Value,
                        getLinkedId,
                        (linkedDocuments) => onFound(linkedDocuments.Append(document).ToArray()),
                        () => onFound(new TDoc[] { document })); // TODO: Log data inconsistency
                },
                () => startDocNotFound().ToTask());

            return result;
        }
        
        public static async Task<TResult> FindRecursiveDocumentsAsync<TDoc, TResult>(this AzureStorageRepository repo,
            Guid startingDocumentId,
            Func<TDoc, Guid[]> getLinkedIds,
            Func<TDoc[], TResult> onFound,
            Func<TResult> startDocNotFound)
            where TDoc : class, ITableEntity
        {
            var result = await await repo.FindByIdAsync(startingDocumentId,
                async (TDoc document) =>
                {
                    var linkedDocIds = getLinkedIds(document);
                    var docs = await linkedDocIds.Select(
                        linkedDocId =>
                            repo.FindRecursiveDocumentsAsync(linkedDocId,
                                getLinkedIds,
                                    (linkedDocuments) => linkedDocuments,
                                    () => (new TDoc[] { })))
                         .WhenAllAsync()
                         .SelectManyAsync()
                         .ToArrayAsync(); // TODO: Log data inconsistency
                    return onFound(docs.Append(document).ToArray());
                },
                () => startDocNotFound().ToTask());

            return result;
        }
    }
}
