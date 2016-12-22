using BlackBarLabs.Persistence.Azure.StorageTables;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
    }
}
