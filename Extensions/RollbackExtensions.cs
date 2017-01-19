using BlackBarLabs.Extensions;
using BlackBarLabs.Persistence.Azure.StorageTables;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace BlackBarLabs.Persistence.Azure
{
    public static class RollbackExtensions
    {
        public static void AddTaskUpdate<TRollback, TDocument>(this RollbackAsync<TRollback> rollback, 
            Guid docId,
            Action<TDocument> mutateUpdate,
            Action<TDocument> mutateRollback,
            Func<TRollback> onNotFound,
            AzureStorageRepository repo)
            where TDocument : class, ITableEntity
        {
            rollback.AddTask(
                async (success, failure) =>
                {
                    var r = await repo.UpdateAsync<TDocument, bool>(docId,
                        async (doc, save) =>
                        {
                            mutateUpdate(doc);
                            await save(doc);
                            return true;
                        },
                        () => false);
                    if (r)
                        return success(
                            async () =>
                            {
                                await repo.UpdateAsync<TDocument, bool>(docId,
                                    async (doc, save) =>
                                    {
                                        mutateRollback(doc);
                                        await save(doc);
                                        return true;
                                    },
                                    () => false);
                            });
                    return failure(onNotFound());
                });
        }

        public static void AddTaskUpdate<TRollback, TDocument>(this RollbackAsync<TRollback> rollback,
            Guid docId,
            Func<TDocument, bool> mutateUpdate,
            Action<TDocument> mutateRollback,
            Func<TRollback> onNotFound,
            Func<TRollback> onMutateFailed,
            AzureStorageRepository repo)
            where TDocument : class, ITableEntity
        {
            rollback.AddTask(
                async (success, failure) =>
                {
                    var r = await repo.UpdateAsync<TDocument, int>(docId,
                        async (doc, save) =>
                        {
                            if (!mutateUpdate(doc))
                                return 1;
                            await save(doc);
                            return 0;
                        },
                        () => -1);
                    if (r == 0)
                        return success(
                            async () =>
                            {
                                await repo.UpdateAsync<TDocument, bool>(docId,
                                    async (doc, save) =>
                                    {
                                        mutateRollback(doc);
                                        await save(doc);
                                        return true;
                                    },
                                    () => false);
                            });
                    if (r == 1)
                        return failure(onMutateFailed());

                    return failure(onNotFound());
                });
        }

        private struct Carry<T>
        {
            public T carry;
        }

        public static void AddTaskUpdate<T, TRollback, TDocument>(this RollbackAsync<TRollback> rollback,
            Guid docId,
            Func<TDocument, T> mutateUpdate,
            Func<T, TDocument, bool> mutateRollback,
            Func<TRollback> onNotFound,
            AzureStorageRepository repo)
            where TDocument : class, ITableEntity
        {
            rollback.AddTask(
                async (success, failure) =>
                {
                    var r = await repo.UpdateAsync<TDocument, Carry<T>?>(docId,
                        async (doc, save) =>
                        {
                            var carry = mutateUpdate(doc);
                            await save(doc);
                            return new Carry<T>
                            {
                                carry = carry,
                            };
                        },
                        () => default(Carry<T>?));
                    if (r.HasValue)
                        return success(
                            async () =>
                            {
                                await repo.UpdateAsync<TDocument, bool>(docId,
                                    async (doc, save) =>
                                    {
                                        mutateRollback(r.Value.carry, doc);
                                        await save(doc);
                                        return true;
                                    },
                                    () => false);
                            });
                    return failure(onNotFound());
                });
        }

        public static void AddTaskUpdate<T, TRollback, TDocument>(this RollbackAsync<T, TRollback> rollback,
            Guid docId,
            Func<TDocument, T> mutateUpdate,
            Func<T, TDocument, bool> mutateRollback,
            Func<TRollback> onNotFound,
            AzureStorageRepository repo)
            where TDocument : class, ITableEntity
        {
            rollback.AddTask(
                async (success, failure) =>
                {
                    var r = await repo.UpdateAsync<TDocument, Carry<T>?>(docId,
                        async (doc, save) =>
                        {
                            var carry = mutateUpdate(doc);
                            await save(doc);
                            return new Carry<T>
                            {
                                carry = carry,
                            };
                        },
                        () => default(Carry<T>?));
                    if (r.HasValue)
                        return success(r.Value.carry,
                            async () =>
                            {
                                await repo.UpdateAsync<TDocument, bool>(docId,
                                    async (doc, save) =>
                                    {
                                        mutateRollback(r.Value.carry, doc);
                                        await save(doc);
                                        return true;
                                    },
                                    () => false);
                            });
                    return failure(onNotFound());
                });
        }

        public static void AddTaskDeleteJoin<TRollback, TDocument>(this RollbackAsync<Guid?, TRollback> rollback,
            Guid docId,
            Func<TDocument, Guid?> mutateDelete,
            Action<Guid, TDocument> mutateRollback,
            Func<TRollback> onNotFound,
            AzureStorageRepository repo)
            where TDocument : class, ITableEntity
        {
            rollback.AddTaskUpdate(docId,
                (TDocument doc) =>
                {
                    var joinId = mutateDelete(doc);
                    return joinId;
                },
                (joinId, doc) =>
                {
                    if (joinId.HasValue)
                    {
                        mutateRollback(joinId.Value, doc);
                        return true;
                    }
                    return false;
                },
                onNotFound,
                repo);
        }

        public static void AddTaskCheckup<TRollback, TDocument>(this RollbackAsync<TRollback> rollback,
            Guid docId,
            Func<TRollback> onDoesNotExists,
            AzureStorageRepository repo)
            where TDocument : class, ITableEntity
        {
            rollback.AddTask(
                async (success, failure) =>
                {
                    return await repo.FindByIdAsync(docId,
                        (TDocument doc) => success(() => 1.ToTask()), () => failure(onDoesNotExists()));
                });
        }

        public static void AddTaskCreate<TRollback, TDocument>(this RollbackAsync<TRollback> rollback,
            Guid docId, TDocument document,
            Func<TRollback> onAlreadyExists,
            AzureStorageRepository repo)
            where TDocument : class, ITableEntity
        {
            rollback.AddTask(
                async (success, failure) =>
                {
                    return await repo.CreateAsync(docId, document,
                        () => success(
                            async () =>
                            {
                                await repo.DeleteIfAsync<TDocument, bool>(docId,
                                    async (doc, delete) => { await delete(); return true; },
                                    () => false);
                            }),
                        () => failure(onAlreadyExists()));
                });
        }

        public static void AddTaskCreateOrUpdate<TRollback, TDocument>(this RollbackAsync<TRollback> rollback,
            Guid docId,
            Func<TDocument, bool> isValidAndMutate,
            Func<TDocument, bool> mutateRollback,
            Func<TRollback> onFail,
            AzureStorageRepository repo)
            where TDocument : class, ITableEntity
        {
            rollback.AddTask(
                (success, failure) =>
                {
                    return repo.CreateOrUpdateAsync<TDocument, RollbackAsync<TRollback>.RollbackResult>(docId,
                        async (created, doc, save) =>
                        {
                            if (!isValidAndMutate(doc))
                                return failure(onFail());
                            
                            await save(doc);
                            return success(
                                async () =>
                                {
                                    if (created)
                                    {
                                        await repo.DeleteIfAsync<TDocument, bool>(docId,
                                            async (docDelete, delete) =>
                                            {
                                                // TODO: Check etag if(docDelete.ET)
                                                await delete();
                                                return true;
                                            },
                                            () => false);
                                        return;
                                    }
                                    await repo.UpdateAsync<TDocument, bool>(docId,
                                        async (docRollback, saveRollback) =>
                                        {
                                            if(mutateRollback(docRollback))
                                                await saveRollback(docRollback);
                                            return true;
                                        },
                                        () => false);
                                });
                        });
                });
        }

        public static async Task<TRollback> ExecuteAsync<TRollback>(this RollbackAsync<TRollback> rollback,
            Func<TRollback> onSuccess)
        {
            return await rollback.ExecuteAsync(onSuccess, r => r);
        }

        public static async Task<TRollback> ExecuteDeleteJoinAsync<TRollback, TDocument>(this RollbackAsync<Guid?, TRollback> rollback,
            Func<TRollback> onSuccess,
            AzureStorageRepository repo)
            where TDocument : class, ITableEntity
        {
            var result = await await rollback.ExecuteAsync<Task<TRollback>>(
                async (joinIds) =>
                {
                    var joinId = joinIds.First(joinIdCandidate => joinIdCandidate.HasValue);
                    if (!joinId.HasValue)
                        return onSuccess();
                    return await repo.DeleteIfAsync<TDocument, TRollback>(joinId.Value,
                        async (doc, delete) =>
                        {
                            await delete();
                            return onSuccess();
                        },
                        () =>
                        {
                            // TODO: Log data inconsistency
                            return onSuccess();
                        });
                },
                (failureResult) => failureResult.ToTask());
            return result;
        }
    }
}
