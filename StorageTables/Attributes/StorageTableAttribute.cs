using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using BlackBarLabs.Persistence.Azure.StorageTables;
using EastFive.Collections.Generic;
using EastFive.Extensions;
using EastFive.Linq;
using EastFive.Linq.Expressions;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace EastFive.Persistence.Azure.StorageTables
{
    public class StorageTableAttribute : Attribute, IProvideTable, IProvideEntity
    {
        public string TableName { get; set; }

        public ITableEntity GetEntity<TEntity>(TEntity entity, string etag = "*")
        {
            var creatableEntity = new TableEntity<TEntity>();
            creatableEntity.Entity = entity;
            creatableEntity.ETag = etag;
            return creatableEntity;
        }

        public TEntity CreateEntityInstance<TEntity>(string rowKey, string partitionKey,
            IDictionary<string, EntityProperty> properties,
            string etag, DateTimeOffset lastUpdated)
        {
            return TableEntity<TEntity>.CreateEntityInstance(rowKey, partitionKey, properties);
        }

        public CloudTable GetTable(Type tableType, CloudTableClient client)
        {
            if (tableType.IsSubClassOfGeneric(typeof(TableEntity<>)))
            {
                var genericTableType = tableType.GenericTypeArguments.First();
                return this.GetTable(genericTableType, client);
            }
            var tableName = this.TableName.HasBlackSpace()?
                this.TableName
                :
                tableType.Name.ToLower();
            var table = client.GetTableReference(tableName);
            return table;
        }

        private class TableEntity<EntityType> : ITableEntity
        {
            public EntityType Entity { get; set; }

            private static TResult GetMemberSupportingInterface<TInterface, TResult>(
                Func<MemberInfo, TInterface, TResult> onFound,
                Func<TResult> onNotFound)
            {
                return typeof(EntityType)
                    .GetMembers()
                    .SelectMany(
                        memberInfo =>
                        {
                            return memberInfo.GetAttributesInterface<TInterface>()
                                .Select(rowKeyModifier => rowKeyModifier.PairWithKey(memberInfo));
                        })
                    .First(
                        (propertyInterfaceKvp, next) =>
                        {
                            var property = propertyInterfaceKvp.Key;
                            var interfaceInstance = propertyInterfaceKvp.Value;
                            return onFound(property, interfaceInstance);
                        },
                        onNotFound);
            }

            public virtual string RowKey
            {
                get
                {
                    return GetMemberSupportingInterface<IModifyAzureStorageTableRowKey, string>(
                        (rowKeyProperty,  rowKeyGenerator) =>
                        {
                            var rowKeyValue = rowKeyGenerator.GenerateRowKey(this.Entity, rowKeyProperty);
                            return rowKeyValue;
                        },
                        () => throw new Exception("Entity does not contain row key attribute"));
                }
                set
                {
                    this.Entity = SetRowKey(this.Entity, value);
                }
            }

            public string PartitionKey
            {
                get
                {
                    return GetMemberSupportingInterface<IModifyAzureStorageTablePartitionKey, string>(
                        (partitionKeyProperty, partitionKeyGenerator) =>
                        {
                            var partitionKey = partitionKeyGenerator.GeneratePartitionKey(this.RowKey, this.Entity, partitionKeyProperty);
                            return partitionKey;
                        },
                        () => throw new Exception("Entity does not contain partition key attribute"));
                }
                set
                {
                    this.Entity = SetPartitionKey(this.Entity, value);
                    //bool found = GetMemberSupportingInterface<IModifyAzureStorageTablePartitionKey, bool>(
                    //    (partitionKeyProperty, partitionKeyGenerator) =>
                    //    {
                    //        partitionKeyGenerator.ParsePartitionKey(this.Entity, value, partitionKeyProperty);
                    //        return true;
                    //    },
                    //    () => throw new Exception("Entity does not contain row key attribute"));
                }
            }

            private static EntityType SetRowKey(EntityType entity, string value)
            {
                return GetMemberSupportingInterface<IModifyAzureStorageTableRowKey, EntityType>(
                    (rowKeyProperty, rowKeyGenerator) =>
                    {
                        return rowKeyGenerator.ParseRowKey(entity, value, rowKeyProperty);
                    },
                    () => throw new Exception("Entity does not contain row key attribute"));
            }

            private static EntityType SetPartitionKey(EntityType entity, string value)
            {
                return GetMemberSupportingInterface<IModifyAzureStorageTablePartitionKey, EntityType>(
                    (partitionKeyProperty, partitionKeyGenerator) =>
                    {
                        return partitionKeyGenerator.ParsePartitionKey(entity, value, partitionKeyProperty);
                    },
                    () => throw new Exception("Entity does not contain row key attribute"));
            }

            public DateTimeOffset Timestamp { get; set; }

            public virtual string ETag { get; set; }

            private IEnumerable<KeyValuePair<MemberInfo, IPersistInAzureStorageTables>> StorageProperties
            {
                get
                {
                    var type = typeof(EntityType);
                    return type.StorageProperties();
                }
            }

            public void ReadEntity(
                IDictionary<string, EntityProperty> properties, OperationContext operationContext)
            {
                this.Entity = CreateEntityInstance(properties);
            }

            public static EntityType CreateEntityInstance(string rowKey, string partitionKey, IDictionary<string, EntityProperty> properties)
            {
                var entity = CreateEntityInstance(properties);
                entity = SetRowKey(entity, rowKey);
                entity = SetPartitionKey(entity, partitionKey);
                return entity;
            }

            public static EntityType CreateEntityInstance(IDictionary<string, EntityProperty> properties)
            {
                var entity = Activator.CreateInstance<EntityType>();
                var storageProperties = typeof(EntityType).StorageProperties();
                foreach (var propInfoAttribute in storageProperties)
                {
                    var propInfo = propInfoAttribute.Key;
                    var attr = propInfoAttribute.Value;
                    var value = attr.GetMemberValue(propInfo, properties);
                    propInfo.SetValue(ref entity, value);
                }
                return entity;
            }

            public IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
            {
                var valuesToStore = StorageProperties
                    .SelectMany(
                        (propInfoAttribute) =>
                        {
                            var propInfo = propInfoAttribute.Key;
                            var attr = propInfoAttribute.Value;
                            var value = propInfo.GetValue(this.Entity);
                            return attr.ConvertValue(value, propInfo);
                        })
                    .ToDictionary();
                return valuesToStore;
            }

        }
    }
}
