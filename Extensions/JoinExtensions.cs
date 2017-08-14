using BlackBarLabs.Persistence.Azure.StorageTables;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BlackBarLabs.Persistence.Azure
{
    public static class JoinExtensions
    {
        public static async Task<TResult> DeleteJoinAsync<TDocument, TResult>(this AzureStorageRepository repo,
            Guid joinId,
            Func<TDocument, Func<bool>, Func<bool>, Func<bool>, Task<bool>> callback,
            Func<TResult> success,
            Func<TResult> notFound)
            where TDocument : class, ITableEntity
        {
            var result = await repo.DeleteIfAsync<TDocument, TResult>(joinId,
                async (joinDoc, deleteJoinDoc) =>
                {
                    var task = deleteJoinDoc();
                    await callback(joinDoc,
                        () => true,
                        () =>
                        {
                            // TODO: Log data consitency error
                            return true;
                        },
                        () =>
                        {
                            // TODO: Log data consitency error
                            return true;
                        });
                    await task;
                    return success();
                },
                notFound);
            return result;
        }

        public static async Task<TResult> DeleteJoinAsync<TLink, TLinkDocument, TLinkedDocument, TResult>(this AzureStorageRepository repo,
            IEnumerable<TLink> links,
            Func<TLink, Guid> getLinkId,
            Func<TLink, Guid> getLinkedId,
            Action<TLinkedDocument> mutateAsync,
            Func<bool, TResult> success)
            where TLinkDocument : class, ITableEntity
            where TLinkedDocument : class, ITableEntity
        {
            var deletedCleans = await links
                .Select(
                    async link =>
                    {
                        var deleteSuccess = repo.DeleteIfAsync<TLinkDocument, bool>(getLinkId(link),
                            async (doc, deleteAsync) =>
                            {
                                await deleteAsync();
                                return true;
                            },
                            () => false);
                        var updateSuccess = await repo.UpdateAsync<TLinkedDocument, bool>(getLinkedId(link),
                            async (doc, updateAsync) =>
                            {
                                mutateAsync(doc);
                                await updateAsync(doc);
                                return true;
                            },
                            () => false);
                        return (await deleteSuccess) && (updateSuccess);
                    })
                    .WhenAllAsync();
            var completeSuccess = deletedCleans.All(t => t);
            return success(completeSuccess);
        }

        public static async Task<TResult> AddJoinAsync<TJoin, TDocJoin, TDoc1, TDoc2, TResult>(this AzureStorageRepository repo,
            Guid id, Guid id1, Guid id2, TDocJoin document,
            Func<TDoc1, TJoin[]> getJoins1,
            Func<TDoc2, TJoin[]> getJoins2,
            Func<TJoin, Guid> id1FromJoin,
            Func<TJoin, Guid> id2FromJoin,
            Action<TDoc1> mutateUpdate1,
            Action<TDoc1> mutateRollback1,
            Action<TDoc2> mutateUpdate2,
            Action<TDoc2> mutateRollback2,
            Func<TResult> onSuccess,
            Func<TResult> joinIdAlreadyExist,
            Func<TJoin, TResult> joinAlreadyExist,
            Func<TResult> doc1DoesNotExist,
            Func<TResult> doc2DoesNotExist)
            where TDocJoin : class, ITableEntity
            where TDoc1 : class, ITableEntity
            where TDoc2 : class, ITableEntity
        {
            var parallel = new RollbackAsync<TResult>();

            var duplicateJoin1 = default(TJoin);
            parallel.AddTaskUpdate<Guid, TResult, TDoc1>(id1,
                (doc, successSave, successNoSave, reject) =>
                {
                    var matches = getJoins1(doc).Where(join => id2FromJoin(join) == id2).ToArray();
                    if (matches.Length > 0)
                    {
                        duplicateJoin1 = matches[0];
                        return reject();
                    }
                    mutateUpdate1(doc);
                    return successSave(id1);
                },
                (id1Again, doc) => { mutateRollback1(doc); return true; },
                () => joinAlreadyExist(duplicateJoin1),
                doc1DoesNotExist,
                repo);

            var duplicateJoin2 = default(TJoin);
            parallel.AddTaskUpdate<Guid, TResult, TDoc2>(id2,
                (doc, successSave, successNoSave, reject) =>
                {
                    var matches = getJoins2(doc).Where(join => id1FromJoin(join) == id1).ToArray();
                    if (matches.Length > 0)
                    {
                        duplicateJoin2 = matches[0];
                        return reject();
                    }
                    mutateUpdate2(doc);
                    return successSave(id2);
                },
                (id1Again, doc) => { mutateRollback2(doc); return true; },
                () => joinAlreadyExist(duplicateJoin2),
                doc2DoesNotExist,
                repo);

            //parallel.AddTaskUpdate(id2,
            //    mutateUpdate2,
            //    mutateRollback2,
            //    doc2DoesNotExist,
            //    repo);

            parallel.AddTaskCreate(id, document,
                () => joinIdAlreadyExist(),
                repo);

            var result = await parallel.ExecuteAsync(
                () => onSuccess(),
                (failureResult) => failureResult);
            return result;
        }
    }
}
