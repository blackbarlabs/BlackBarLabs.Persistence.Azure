using EastFive.Persistence.Azure.StorageTables.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

using Microsoft.WindowsAzure.Storage.Table;

namespace EastFive.Persistence.Azure.StorageTables
{
    public interface IModifyAzureStorageTableSave
    {
        Task<TResult> ExecuteCreateAsync<TEntity, TResult>(MemberInfo memberInfo,
                string rowKeyRef, string partitionKeyRef,
                TEntity value, IDictionary<string, EntityProperty> dictionary,
                AzureTableDriverDynamic repository,
            Func<Func<Task>, TResult> onSuccessWithRollback,
            Func<TResult> onFailure);

        Task<TResult> ExecuteUpdateAsync<TEntity, TResult>(MemberInfo memberInfo,
                string rowKeyRef, string partitionKeyRef,
                TEntity valueExisting, IDictionary<string, EntityProperty> dictionaryExisting,
                TEntity valueUpdated, IDictionary<string, EntityProperty> dictionaryUpdated,
                AzureTableDriverDynamic repository,
            Func<Func<Task>, TResult> onSuccessWithRollback,
            Func<TResult> onFailure);

        Task<TResult> ExecuteDeleteAsync<TEntity, TResult>(MemberInfo memberInfo,
                string rowKeyRef, string partitionKeyRef,
                TEntity value, IDictionary<string, EntityProperty> dictionary,
                AzureTableDriverDynamic repository,
            Func<Func<Task>, TResult> onSuccessWithRollback,
            Func<TResult> onFailure);
    }
}
