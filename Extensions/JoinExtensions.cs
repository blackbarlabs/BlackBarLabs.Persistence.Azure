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

        public static async Task<TResult> AddJoinAsync<TJoin, TDocJoin, TDoc1, TDoc2, TResult>(this AzureStorageRepository repo,
            Guid id, Guid id1, Guid id2, TDocJoin document,
            Func<TDoc1, TJoin[]> getJoins,
            Func<TJoin, Guid> idFromJoin,
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

            var duplicateJoin = default(TJoin);
            parallel.AddTaskUpdate(id1,
                (TDoc1 doc) =>
                {
                    var matches = getJoins(doc).Where(join => id2FromJoin(join) == id2).ToArray();
                    if (matches.Length > 0)
                    {
                        duplicateJoin = matches[0];
                        return false;
                    }
                    mutateUpdate1(doc);
                    return true;
                },
                mutateRollback1,
                doc1DoesNotExist,
                () => joinAlreadyExist(duplicateJoin),
                repo);

            parallel.AddTaskUpdate(id2,
                mutateUpdate2,
                mutateRollback2,
                doc2DoesNotExist,
                repo);
            
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
