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

        public static TResult Connection<TResult>(Func<AzureStorageRepository, TResult> onConnected)
        {
            var repo = AzureStorageRepository.CreateRepository(EastFive.Azure.Persistence.AppSettings.Storage);

            return onConnected(repo);
        }
    }
}
