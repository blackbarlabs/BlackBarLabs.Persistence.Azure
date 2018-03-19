using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BlackBarLabs.Persistence.Azure.StorageTables
{
    public partial class AzureStorageRepository
    {
		public async Task<bool> PurgeAsync()
        {
            var deleteTasks = this.TableClient.ListTables().Select(
                table => table.DeleteAsync());
            await Task.WhenAll(deleteTasks);
            return true;
        }

        public Task<Task> FindLinkedDocumentsAsync<T1, T2, T3>(Guid actorId, string systemName, Func<T1, Guid[]> p1, Func<object, object, Task<object>> p2, Func<Task<object>> p3)
        {
            throw new NotImplementedException();
        }

        public Task<TResult> LockedUpdateAsync<T1, TResult>(Guid credentialId, Func<T1, bool> p1, Func<T1, object, object, Task<bool>> p2, Func<TResult> onNotFound, Func<TResult> onLockRejected)
        {
            throw new NotImplementedException();
        }
    }
}
