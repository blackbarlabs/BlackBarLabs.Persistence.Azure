using BlackBarLabs.Extensions;
using BlackBarLabs.Persistence.Azure.StorageTables;
using EastFive;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace BlackBarLabs.Persistence.Azure
{
    public static class RollbackExtensions
    {
        public static void AddTaskUpdate<TRollback, TDocument>(this BlackBarLabs.Persistence.RollbackAsync<TRollback> rollback, 
            Guid docId,
            Action<TDocument> mutateUpdate,
            Action<TDocument> mutateRollback,
            Func<TRollback> onNotFound,
            AzureStorageRepository repo)
            where TDocument : class, ITableEntity
        {
            rollback.AddTaskUpdate<bool, TRollback, TDocument>(docId,
                (doc, save, successNoSave, fail) => { mutateUpdate(doc); return save(false); },
                (throwAway, doc) => { mutateRollback(doc); return true; },
                "This version of update does not support a failure case".AsFunctionException<TRollback>(), // should never happen
                onNotFound,
                repo);
        }

        public static void AddTaskUpdate<TRollback, TDocument>(this RollbackAsync<TRollback> rollback,
            Guid docId,
            Func<
                    TDocument,
                    Func<UpdateCallback<bool>>, // Save + Success
                    Func<UpdateCallback<bool>>, // No Save + Success
                UpdateCallback<bool>> mutateUpdate,
            Func<TDocument, bool> mutateRollback,
            Func<TRollback> onNotFound,
            AzureStorageRepository repo)
            where TDocument : class, ITableEntity
        {
            rollback.AddTaskUpdate<bool, TRollback, TDocument>(docId,
                (doc, save, successNoSave, reject) => mutateUpdate(doc,
                    () => save(true),
                    () => successNoSave()),
                (throwAway, doc) => { mutateRollback(doc); return true; },
                "This version of update does not support a failure case".AsFunctionException<TRollback>(), // should never happen
                onNotFound,
                repo);
        }

        public static void AddTaskUpdate<TRollback, TDocument>(this RollbackAsync<TRollback> rollback,
            Guid docId,
            Func<
                    TDocument,
                    Func<UpdateCallback<bool>>, // Save + Success
                    Func<UpdateCallback<bool>>, // No Save + Success
                    Func<UpdateCallback<bool>>, // Reject
                UpdateCallback<bool>> mutateUpdate,
            Func<TDocument, bool> mutateRollback,
            Func<TRollback> onMutateRejected,
            Func<TRollback> onNotFound,
            AzureStorageRepository repo)
            where TDocument : class, ITableEntity
        {
            rollback.AddTaskUpdate<bool, TRollback, TDocument>(docId,
                (doc, save, successNoSave, reject) => mutateUpdate(doc,
                    () => save(true),
                    () => successNoSave(),
                    () => reject()),
                (throwAway, doc) => { mutateRollback(doc); return true; },
                onMutateRejected,
                onNotFound,
                repo);
        }

        public static void AddTaskAsyncUpdate<TRollback, TDocument>(this RollbackAsync<TRollback> rollback,
            Guid docId,
            Func<TDocument, Task<bool>> mutateUpdate,
            Func<TDocument, Task> mutateRollback,
            Func<TRollback> onNotFound,
            AzureStorageRepository repo)
            where TDocument : class, ITableEntity
        {
            rollback.AddTask(
                async (success, failure) =>
                {
                    var r = await repo.UpdateAsync<TDocument, int>(docId,
                        async (doc, save) =>
                        {
                            if (!await mutateUpdate(doc))
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
                                        await mutateRollback(doc);
                                        await save(doc);
                                        return true;
                                    },
                                    () => false);
                            });
                    if (r == 1)
                        return success(() => true.ToTask());

                    return failure(onNotFound());
                });
        }

        private struct Carry<T>
        {
            public T carry;
        }

        public class UpdateCallback<T>
        {
            private UpdateCallback(T t, bool success, bool found, bool save, bool reject)
            {
                this.t = t;
                this.success = success;
                this.found = found;
                this.save = save;
                this.rejected = reject;
            }

            private UpdateCallback(bool success, bool found, bool save, bool reject)
            {
                this.t = default(T);
                this.success = success;
                this.found = found;
                this.save = save;
                this.rejected = reject;
            }

            internal T t;
            internal bool success;
            internal bool found;
            internal bool save;
            internal bool rejected;

            internal static UpdateCallback<T> Save(T t)
            {
                return new UpdateCallback<T>(t, true, true, true, false);
            }
            internal static UpdateCallback<T> SuccessNoSave()
            {
                return new UpdateCallback<T>(true, true, false, false);
            }
            internal static UpdateCallback<T> NotFound()
            {
                return new UpdateCallback<T>(false, false, false, false);
            }

            internal static UpdateCallback<T> Reject()
            {
                return new UpdateCallback<T>(false, true, false, true);
            }
        }

        public static void AddTaskUpdate<T, TRollback, TDocument>(this RollbackAsync<TRollback> rollback,
            Guid docId,
            Func<
                    TDocument,
                    Func<T, UpdateCallback<T>>, // Save + Success
                    Func<UpdateCallback<T>>, // No Save + Success
                UpdateCallback<T>> mutateUpdate,
            Func<T, TDocument, bool> mutateRollback,
            Func<TRollback> onNotFound,
            AzureStorageRepository repo)
            where TDocument : class, ITableEntity
        {
            rollback.AddTaskUpdate(docId,
                (doc, save, successNoSave, fail) =>
                    mutateUpdate(doc, save, successNoSave),
                mutateRollback,
                "This version of update does not support a failure case".AsFunctionException<TRollback>(), // should never happen
                onNotFound,
                repo);
        }

        public static void AddTaskUpdate<T, TRollback, TDocument>(this RollbackAsync<TRollback> rollback,
            Guid docId,
            Func<
                    TDocument,
                    Func<T, UpdateCallback<T>>, // Save + Success
                    Func<UpdateCallback<T>>, // No Save + Success
                    Func<UpdateCallback<T>>, // Reject
                UpdateCallback<T>> mutateUpdate,
            Func<T, TDocument, bool> mutateRollback,
            Func<TRollback> onMutateRejected,
            Func<TRollback> onNotFound,
            AzureStorageRepository repo)
            where TDocument : class, ITableEntity
        {
            rollback.AddTask(
                async (success, failure) =>
                {
                    var r = await repo.UpdateAsync<TDocument, UpdateCallback<T>>(docId,
                        async (doc, save) =>
                        {
                            var passThrough = mutateUpdate(doc,
                                (passThroughSuccess) => UpdateCallback<T>.Save(passThroughSuccess),
                                () => UpdateCallback<T>.SuccessNoSave(),
                                () => UpdateCallback<T>.Reject());
                            if(passThrough.save)
                                await save(doc);
                            return passThrough;
                        },
                        () => UpdateCallback<T>.NotFound());

                    if (!r.found)
                        return failure(onNotFound());
                    if (!r.success)
                        return failure(onMutateRejected());

                    return success(
                        async () =>
                        {
                            if (r.save)
                                await repo.UpdateAsync<TDocument, bool>(docId,
                                    async (doc, save) =>
                                    {
                                        if (mutateRollback(r.t, doc))
                                            await save(doc);
                                        return true;
                                    },
                                    () => false);

                            // If this was not saved, there is no reason to do anything on the rollback
                        });

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

        public static void AddTaskUpdate<T, TRollback, TDocument>(this RollbackAsync<T, TRollback> rollback,
                Guid docId,
            Func<TDocument, T> mutateUpdate,
            Func<T, TDocument, bool> mutateRollback,
            Func<T> ifNotFound,
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
                    return success(ifNotFound(), () => ((object)null).ToTask());
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
                () => default(Guid?),
                repo);
        }

        public static void AddTaskDeleteJoin<TRollback, TDocument>(this RollbackAsync<Guid?, TRollback> rollback,
            Guid docId,
            Func<TDocument, Guid?> removeLink,
            Action<Guid, TDocument> rollbackLink,
            AzureStorageRepository repo)
            where TDocument : class, ITableEntity
        {
            rollback.AddTaskUpdate(docId,
                (TDocument doc) =>
                {
                    var joinId = removeLink(doc);
                    return joinId;
                },
                (joinId, doc) =>
                {
                    if (joinId.HasValue)
                    {
                        rollbackLink(joinId.Value, doc);
                        return true;
                    }
                    return false;
                },
                () => default(Guid?),
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
            AzureStorageRepository repo,
            Func<string, string> mutatePartition = default(Func<string, string>))
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
                                    () => false,
                                    mutatePartition: mutatePartition);
                            }),
                        () => failure(onAlreadyExists()),
                        mutatePartition:mutatePartition);
                });
        }

        public static void AddTaskCreate<TRollback, TDocument>(this RollbackAsync<TRollback> rollback,
            Guid docId, string partitionKey, TDocument document,
            Func<TRollback> onAlreadyExists,
            AzureStorageRepository repo)
            where TDocument : class, ITableEntity
        {
            rollback.AddTask(
                async (success, failure) =>
                {
                    return await repo.CreateAsync(docId, partitionKey, document,
                        () => success(
                            async () =>
                            {
                                await repo.DeleteIfAsync<TDocument, bool>(docId, partitionKey,
                                    async (doc, delete) => { await delete(); return true; },
                                    () => false);
                            }),
                        () => failure(onAlreadyExists()));
                });
        }

        public static void AddTaskCreate<TRollback, TDocument>(this RollbackAsync<TRollback> rollback,
            string rowKey, string partitionKey, TDocument document,
            Func<TRollback> onAlreadyExists,
            AzureStorageRepository repo)
            where TDocument : class, ITableEntity
        {
            rollback.AddTask(
                async (success, failure) =>
                {
                    return await repo.CreateAsync(rowKey, partitionKey, document,
                        () => success(
                            async () =>
                            {
                                await repo.DeleteIfAsync<TDocument, bool>(rowKey, partitionKey,
                                    async (doc, delete) =>
                                    {
                                        await delete();
                                        return true;
                                    },
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
        
        public static void AddTaskCreateOrUpdate<TRollback, TDocument>(this RollbackAsync<TRollback> rollback,
            Guid docId,
            Func<bool, TDocument, bool> isMutated,
            Action<TDocument> mutateRollback,
            AzureStorageRepository repo)
            where TDocument : class, ITableEntity
        {
            rollback.AddTask(
                (success, failure) =>
                {
                    return repo.CreateOrUpdateAsync<TDocument, RollbackAsync<TRollback>.RollbackResult>(docId,
                        async (created, doc, save) =>
                        {
                            if (!isMutated(created, doc))
                                return success(
                                    async () =>
                                    {
                                        if (created)
                                            await repo.DeleteAsync(doc,
                                                () => true,
                                                () => false);
                                    });

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
                                            mutateRollback(docRollback);
                                            await saveRollback(docRollback);
                                            return true;
                                        },
                                        () => false);
                                });
                        });
                });
        }


        public static void AddTaskCreateOrUpdate<TRollback, TDocument>(this RollbackAsync<TRollback> rollback,
            Guid docId, string partitionKey,
            Func<bool, TDocument, bool> isMutated,
            Action<TDocument> mutateRollback,
            AzureStorageRepository repo)
            where TDocument : class, ITableEntity
        {
            rollback.AddTask(
                (success, failure) =>
                {
                    return repo.CreateOrUpdateAsync<TDocument, RollbackAsync<TRollback>.RollbackResult>(docId, partitionKey,
                        async (created, doc, save) =>
                        {
                            if (!isMutated(created, doc))
                                return success(
                                    async () =>
                                    {
                                        if (created)
                                            await repo.DeleteAsync(doc,
                                                () => true,
                                                () => false);
                                    });

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
                                            mutateRollback(docRollback);
                                            await saveRollback(docRollback);
                                            return true;
                                        },
                                        () => false);
                                });
                        });
                });
        }

        public static void AddTaskAsyncCreateOrUpdate<TRollback, TDocument>(this RollbackAsync<TRollback> rollback,
            Guid docId,
            Func<TDocument, Task<bool>> isValidAndMutate,
            Func<TDocument, Task<bool>> mutateRollback,
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
                            if (!await isValidAndMutate(doc))
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
                                            if (await mutateRollback(docRollback))
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
