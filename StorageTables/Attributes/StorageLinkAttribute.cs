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
using System.Net.Http;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace EastFive.Persistence.Azure.StorageTables
{
    public class StorageLinkAttribute : Attribute,
        IModifyAzureStorageTableSave, IProvideFindBy
    {
        public Type ReferenceType { get; set; }

        public string ReferenceProperty { get; set; }

        public string LookupTableName { get; set; }

        public Type PartitionAttribute { get; set; }

        private string GetLookupTableName(MemberInfo memberInfo)
        {
            if (LookupTableName.HasBlackSpace())
                return this.LookupTableName;
            return $"{memberInfo.DeclaringType.Name}{memberInfo.Name}";
        }

        public IEnumerableAsync<KeyValuePair<string, string>> GetKeys<TEntity>(IRef<TEntity> value, AzureTableDriverDynamic repository, MemberInfo memberInfo)
            where TEntity : struct, IReferenceable
        {
            var tableName = GetLookupTableName(memberInfo);
            var rowKey = value.id.AsRowKey();
            var partitionKey = GetPartitionKey(rowKey, null, memberInfo);
            return repository
                .FindByIdAsync<StorageLookupTable, IEnumerableAsync <KeyValuePair<string, string>>>(rowKey, partitionKey,
                    (dictEntity) =>
                    {
                        var rowAndParitionKeys = dictEntity.rowAndPartitionKeys.NullToEmpty()
                            .Select(rowParitionKeyKvp => rowParitionKeyKvp.AsTask())
                            .AsyncEnumerable();
                        return rowAndParitionKeys;
                    },
                    () => EnumerableAsync.Empty<KeyValuePair<string, string>>(),
                    tableName: tableName)
                .FoldTask();
        }

        [StorageTable]
        public struct StorageLookupTable
        {
            [RowKey]
            public string rowKey;

            [ParititionKey]
            public string partitionKey;

            [Storage]
            public KeyValuePair<string, string>[] rowAndPartitionKeys;
        }

        private string GetPartitionKey(string rowKey, object value, MemberInfo memberInfo)
        {
            var partitionKeyAttributeType = this.PartitionAttribute.IsDefaultOrNull() ?
                  typeof(StandardParititionKeyAttribute)
                  :
                  this.PartitionAttribute;
            if (!partitionKeyAttributeType.IsSubClassOfGeneric(typeof(IModifyAzureStorageTablePartitionKey)))
                throw new Exception($"{memberInfo.DeclaringType.FullName}..{memberInfo.Name} defines partition type as {partitionKeyAttributeType.FullName} which does not implement {typeof(IModifyAzureStorageTablePartitionKey).FullName}.");
            var partitionKeyAttribute = Activator.CreateInstance(partitionKeyAttributeType) as IModifyAzureStorageTablePartitionKey;
            var partitionKey = partitionKeyAttribute.GeneratePartitionKey(rowKey, value, memberInfo);
            return partitionKey;
        }

        public virtual async Task<TResult> ExecuteAsync<TEntity, TResult>(MemberInfo memberInfo,
                string rowKeyRef, string partitionKeyRef,
                TEntity value, IDictionary<string, EntityProperty> dictionary,
                AzureTableDriverDynamic repository,
                Func<IEnumerable<KeyValuePair<string, string>>, IEnumerable<KeyValuePair<string, string>>> mutateCollection,
            Func<Func<Task>, TResult> onSuccessWithRollback,
            Func<TResult> onFailure)
        {
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
            var partitionKey = GetPartitionKey(rowKey, value, memberInfo);
            var tableName = GetLookupTableName(memberInfo);
            return await repository.UpdateOrCreateAsync<StorageLookupTable, TResult>(rowKey, partitionKey,
                async (created, lookup, saveAsync) =>
                {
                    lookup.rowKey = rowKey;
                    lookup.partitionKey = partitionKey;
                    lookup.rowAndPartitionKeys = mutateCollection(lookup.rowAndPartitionKeys)
                        .Distinct(rpKey => rpKey.Key)
                        .ToArray();
                    await saveAsync(lookup);
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
                },
                tableName:tableName);
        }

        public static TResult ModificationFailure<T, TResult>(T item, Func<TResult> resultFunc)
        {
            throw new NotImplementedException();
        }

        public virtual Task<TResult> ExecuteCreateAsync<TEntity, TResult>(MemberInfo memberInfo,
                string rowKeyRef, string partitionKeyRef,
                TEntity value, IDictionary<string, EntityProperty> dictionary,
                AzureTableDriverDynamic repository,
            Func<Func<Task>, TResult> onSuccessWithRollback,
            Func<TResult> onFailure)
        {
            return ExecuteAsync(memberInfo,
                rowKeyRef, partitionKeyRef,
                value, dictionary,
                repository,
                (rowAndParitionKeys) => rowAndParitionKeys.Append(rowKeyRef.PairWithValue(partitionKeyRef)),
                onSuccessWithRollback,
                onFailure);
        }

        public Task<TResult> ExecuteUpdateAsync<TEntity, TResult>(MemberInfo memberInfo, 
                string rowKeyRef, string partitionKeyRef, 
                TEntity valueExisting, IDictionary<string, EntityProperty> dictionaryExisting,
                TEntity valueUpdated, IDictionary<string, EntityProperty> dictionaryUpdated, 
                AzureTableDriverDynamic repository, 
            Func<Func<Task>, TResult> onSuccessWithRollback, 
            Func<TResult> onFailure)
        {
            // Since only updating the row/partition keys could force a change here, just ignroe
            return onSuccessWithRollback(
                () => true.AsTask()).AsTask();
        }

        public Task<TResult> ExecuteDeleteAsync<TEntity, TResult>(MemberInfo memberInfo, 
                string rowKeyRef, string partitionKeyRef,
                TEntity value, IDictionary<string, EntityProperty> dictionary, 
                AzureTableDriverDynamic repository,
            Func<Func<Task>, TResult> onSuccessWithRollback, 
            Func<TResult> onFailure)
        {
            return ExecuteAsync(memberInfo,
                rowKeyRef, partitionKeyRef,
                value, dictionary,
                repository,
                (rowAndParitionKeys) => rowAndParitionKeys.Where(kvp => kvp.Key != rowKeyRef),
                onSuccessWithRollback,
                onFailure);
        }
    }
}
