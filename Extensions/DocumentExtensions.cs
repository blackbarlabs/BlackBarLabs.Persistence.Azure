using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using BlackBarLabs.Persistence.Azure.StorageTables;
using BlackBarLabs.Extensions;
using System.Collections.Generic;
using BlackBarLabs.Collections.Generic;
using BlackBarLabs.Linq;
using EastFive.Linq;
using BlackBarLabs.Linq.Async;
using EastFive.Collections.Generic;
using EastFive.Linq.Async;

namespace BlackBarLabs.Persistence
{
    public static class DocumentExtensions
    {
        public static async Task<TResult> FindLinkedDocumentAsync<TParentDoc, TLinkedDoc, TResult>(this AzureStorageRepository repo,
            TableQuery<TParentDoc> query,
            Func<TParentDoc, Guid> getLinkedId,
            Func<IDictionary<TParentDoc, TLinkedDoc>, TResult> found,
            Func<TResult> parentDocNotFound)
            where TParentDoc : class, ITableEntity, new()
            where TLinkedDoc : class, ITableEntity
        {
            var parentDocs = await repo.FindByQueryAsync(query);
            var results = await parentDocs
                .Aggregate(
                    (new Dictionary<TParentDoc, TLinkedDoc>()).ToTask(),
                    async (docsTask, document) =>
                    {
                        var linkedDocId = getLinkedId(document);
                        return await await repo.FindByIdAsync(linkedDocId,
                            async (TLinkedDoc priceSheetDocument) =>
                            {
                                var docs = await docsTask;
                                docs.Add(document, priceSheetDocument);
                                return docs;
                            },
                            () => docsTask);
                });

            return found(results);
        }

        public static async Task<TResult> FindLinkedDocumentAsync<TParentDoc, TLinkedDoc, TResult>(this AzureStorageRepository repo,
            string parentDocRowKey, string parentDocPartitionKey,
            Func<TParentDoc, Guid> getLinkedId,
            Func<TParentDoc, TLinkedDoc, TResult> found,
            Func<TResult> parentDocNotFound,
            Func<TResult> linkedDocNotFound)
            where TParentDoc : class, ITableEntity
            where TLinkedDoc : class, ITableEntity
        {
            var result = await await repo.FindByIdAsync(parentDocRowKey, parentDocPartitionKey,
                async (TParentDoc document) =>
                {
                    var linkedDocId = getLinkedId(document);
                    return await repo.FindByIdAsync(linkedDocId,
                        (TLinkedDoc priceSheetDocument) => found(document, priceSheetDocument),
                        () => linkedDocNotFound());
                },
                () => parentDocNotFound().ToTask());

            return result;
        }

        public static async Task<TResult> FindLinkedDocumentAsync<TParentDoc, TLinkedDoc, TResult>(this AzureStorageRepository repo,
            Guid parentDocId,
            Func<TParentDoc, Guid> getLinkedId,
            Func<TParentDoc, TLinkedDoc, TResult> found,
            Func<TResult> parentDocNotFound,
            Func<TResult> linkedDocNotFound)
            where TParentDoc : class, ITableEntity
            where TLinkedDoc : class, ITableEntity
        {
            var result = await await repo.FindByIdAsync(parentDocId,
                async (TParentDoc document) =>
                {
                    var linkedDocId = getLinkedId(document);
                    return await repo.FindByIdAsync(linkedDocId,
                        (TLinkedDoc priceSheetDocument) => found(document, priceSheetDocument),
                        () => linkedDocNotFound());
                },
               () => parentDocNotFound().ToTask());

            return result;
        }
        
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

        public static async Task<TResult> FindLinkedDocumentsAsync<TParentDoc, TLinkedDoc, TResult>(this AzureStorageRepository repo,
            Guid parentDocId, string partitionKey,
            Func<TParentDoc, Guid[]> getLinkedIds,
            Func<TParentDoc, TLinkedDoc[], TResult> found,
            Func<TResult> parentDocNotFound)
            where TParentDoc : class, ITableEntity
            where TLinkedDoc : class, ITableEntity
        {
            var result = await await repo.FindByIdAsync(parentDocId, partitionKey,
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

        public static async Task<TResult> FindLinkedLinkedDocumentAsync<TParentDoc, TMiddleDoc, TLinkedDoc, TResult>(this AzureStorageRepository repo,
            Guid parentDocId,
            Func<TParentDoc, Guid> getMiddleDocumentId,
            Func<TMiddleDoc, Guid> getLinkedId,
            Func<TParentDoc, TMiddleDoc, TLinkedDoc, TResult> found,
            Func<TResult> parentDocNotFound,
            Func<TResult> middleDocNotFound,
            Func<TResult> linkedDocNotFound)
            where TParentDoc : class, ITableEntity
            where TMiddleDoc : class, ITableEntity
            where TLinkedDoc : class, ITableEntity
        {
            var result = await await repo.FindByIdAsync(parentDocId,
                async (TParentDoc parentDoc) =>
                {
                    var middleDocId = getMiddleDocumentId(parentDoc);
                    var middleAndLinkedDocs = await repo.FindLinkedDocumentAsync(middleDocId,
                            (middleDoc) => getLinkedId(middleDoc),
                            (TMiddleDoc middleDoc, TLinkedDoc linkedDoc) =>
                        found(parentDoc, middleDoc, linkedDoc),
                        () => middleDocNotFound(),
                        () => linkedDocNotFound());
                    return middleAndLinkedDocs;
                },
                () =>
                {
                    // TODO: Log data inconsistency here
                    return parentDocNotFound().ToTask();
                });
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
                                    () => default(KeyValuePair<TMiddleDoc, TLinkedDoc>?),
                                    () => default(KeyValuePair<TMiddleDoc, TLinkedDoc>?)))
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
            Func<TParentDoc, Guid> getMiddleDocumentId,
            Func<TMiddleDoc, Guid[]> getLinkedIds,
            Func<TParentDoc, TMiddleDoc, TLinkedDoc[], TResult> found,
            Func<TResult> parentDocNotFound,
            Func<TResult> middleDocNotFound,
            Func<TResult> linkedDocNotFound)
            where TParentDoc : class, ITableEntity
            where TMiddleDoc : class, ITableEntity
            where TLinkedDoc : class, ITableEntity
        {
            var resultAll = await await repo.FindByIdAsync(parentDocId,
                async (TParentDoc parentDoc) =>
                {
                    var middleDocId = getMiddleDocumentId(parentDoc);
                    var result = await repo.FindLinkedDocumentsAsync(middleDocId,
                        (middleDoc) => getLinkedIds(middleDoc),
                        (TMiddleDoc middleDoc, TLinkedDoc[] linkedDocsByMiddleDocs) =>
                            found(parentDoc, middleDoc, linkedDocsByMiddleDocs),
                        () => middleDocNotFound());
                    return result;
                },
                () => parentDocNotFound().ToTask());
            return resultAll;
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

        /// <summary>
        /// Starting with <paramref name="startingDocumentId"/>, searches for documents until <paramref name="getLinkedId"/>
        /// returns a Guid? without a value.
        /// </summary>
        /// <typeparam name="TDoc"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="repo"></param>
        /// <param name="startingDocumentId"></param>
        /// <param name="getLinkedId"></param>
        /// <param name="onFound">Passes an array of found documents in reverse order (document with id <paramref name="startingDocumentId"/> is last).</param>
        /// <param name="startDocNotFound"></param>
        /// <returns></returns>
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
                        return onFound(document.AsEnumerable().ToArray());
                    return await repo.FindRecursiveDocumentsAsync(linkedDocId.Value,
                        getLinkedId,
                        (linkedDocuments) => onFound(linkedDocuments.Append(document).ToArray()),
                        () => onFound(new TDoc[] { document })); // TODO: Log data inconsistency
                },
                () => startDocNotFound().ToTask());

            return result;
        }

        public static async Task<TResult> FindRecursiveDocumentsAsync<TDoc, TResult>(this AzureStorageRepository repo,
            Guid startingDocumentId, HashSet<Guid> skip,
            Func<TDoc, Guid?> getLinkedId,
            Func<TDoc[], TResult> onFound,
            Func<TResult> startDocNotFound)
            where TDoc : class, ITableEntity
        {
            var result = await await repo.FindByIdAsync(startingDocumentId,
                async (TDoc document) =>
                {
                    var linkedDocId = getLinkedId(document);
                    if ((!linkedDocId.HasValue) || skip.Contains(linkedDocId.Value))
                        return onFound(document.AsEnumerable().ToArray());
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
