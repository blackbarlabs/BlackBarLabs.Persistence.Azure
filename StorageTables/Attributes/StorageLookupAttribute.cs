using BlackBarLabs.Extensions;
using BlackBarLabs.Persistence.Azure.StorageTables;
using EastFive.Collections.Generic;
using EastFive.Extensions;
using EastFive.Linq;
using EastFive.Linq.Async;
using EastFive.Linq.Expressions;
using EastFive.Persistence.Azure.StorageTables.Driver;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace EastFive.Persistence.Azure.StorageTables
{
    public class StorageLookupAttribute : Attribute,
        IModifyAzureStorageTableSave, IProvideFindBy
    {
        public string LookupTableName { get; set; }

        private MemberInfo memberInfo;
        private object value;

        private string GetLookupTableName(MemberInfo memberInfo)
        {
            if (LookupTableName.HasBlackSpace())
                return this.LookupTableName;
            return $"{memberInfo.DeclaringType.Name}{memberInfo.Name}";
        }

        public virtual async Task<TResult> ExecuteAsync<TEntity, TResult>(MemberInfo memberInfo,
                string rowKeyRef, string partitionKeyRef,
                TEntity value, IDictionary<string, EntityProperty> dictionary,
                AzureTableDriverDynamic repository,
            Func<Func<Task>, TResult> onSuccessWithRollback,
            Func<TResult> onFailure)
        {
            this.memberInfo = memberInfo;
            this.value = value;

            string GetRowKey()
            {
                var rowKeyValue = memberInfo.GetValue(value);
                var propertyValueType = memberInfo.GetMemberType();
                if (typeof(Guid).IsAssignableFrom(propertyValueType))
                {
                    var guidValue = (Guid)rowKeyValue;
                    return guidValue.AsRowKey();
                }
                if (typeof(IReferenceable).IsAssignableFrom(propertyValueType))
                {
                    var refValue = (IReferenceable)rowKeyValue;
                    return refValue.id.AsRowKey();
                }
                if (typeof(string).IsAssignableFrom(propertyValueType))
                {
                    var stringValue = (string)rowKeyValue;
                    return stringValue;
                }
                return rowKeyValue.ToString();
            }
            var rowKey = GetRowKey();
            var partitionKey = StandardParititionKeyAttribute.GetValue(rowKey);

            return await repository.UpdateOrCreateAsync<DictionaryTableEntity<string[]>, TResult>(rowKey, partitionKey,
                async (created, kvps, saveAsync) =>
                {
                    var values = kvps.values.IsDefaultOrNull() ?
                        new Dictionary<string, string[]>()
                        :
                        kvps.values;
                    var rowKeys = values.ContainsKey("rowkeys") ?
                        values["rowkeys"] : new string[] { };
                    var partitionKeys = values.ContainsKey("partitionkeys") ?
                        values["partitionkeys"] : new string[] { };
                    var rowAndParitionKeys = rowKeys
                        .Zip(partitionKeys, (rk, pk) => rk.PairWithValue(pk))
                        .Append(rowKeyRef.PairWithValue(partitionKeyRef))
                        .Distinct(rpKey => rpKey.Key);
                    kvps.values = new Dictionary<string, string[]>()
                    {
                        { "rowkeys", rowAndParitionKeys.SelectKeys().ToArray() },
                        { "partitionkeys", rowAndParitionKeys.SelectValues().ToArray() },
                    };
                    await saveAsync(kvps);
                    Func<Task> rollback =
                        async () =>
                        {
                            if (created)
                            {
                                await repository.DeleteAsync<DictionaryTableEntity<string[]>, bool>(rowKey, partitionKey,
                                    () => true,
                                    () => false);
                                return;
                            }

                            // TODO: Other rollback
                        };
                    return onSuccessWithRollback(rollback);
                });
        }

        public IEnumerableAsync<KeyValuePair<string, string>> GetKeys<TEntity>(IRef<TEntity> value, AzureTableDriverDynamic repository)
            where TEntity : struct, IReferenceable
        {
            var rowKey = value.id.AsRowKey();
            var partitionKey = StandardParititionKeyAttribute.GetValue(rowKey);
            return repository
                .FindByIdAsync<DictionaryTableEntity<string[]>, IEnumerableAsync<KeyValuePair<string, string>>>(rowKey, partitionKey,
                    (dictEntity) =>
                    {
                        var values = dictEntity.values.IsDefaultOrNull() ?
                            new Dictionary<string, string[]>()
                            :
                            dictEntity.values;
                        var rowKeys = values.ContainsKey("rowkeys") ?
                            values["rowkeys"] : new string[] { };
                        var partitionKeys = values.ContainsKey("partitionkeys") ?
                            values["partitionkeys"] : new string[] { };
                        var rowAndParitionKeys = rowKeys
                            .Zip(partitionKeys, (rk, pk) => rk.PairWithValue(pk))
                            .Select(rowParitionKeyKvp => rowParitionKeyKvp.AsTask())
                            .AsyncEnumerable();
                        return rowAndParitionKeys;
                    },
                    () => EnumerableAsync.Empty<KeyValuePair<string, string>>())
                .FoldTask();
        }
    }
}
