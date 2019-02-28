using BlackBarLabs.Persistence.Azure.StorageTables;
using EastFive.Collections.Generic;
using EastFive.Extensions;
using EastFive.Linq;
using EastFive.Linq.Expressions;
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
    public class TableEntity<EntityType> : ITableEntity
    {
        public EntityType Entity { get; private set; }

        public virtual string RowKey
        {
            get
            {
                var properties = typeof(EntityType)
                    .GetMembers()
                    .ToArray();

                var rowKeyModificationProperties = properties
                       .Where(propInfo => propInfo.ContainsAttributeInterface<IModifyAzureStorageTableRowKey>())
                       .Select(propInfo => propInfo.GetAttributesInterface<IModifyAzureStorageTableRowKey>().PairWithKey(propInfo))
                       .Where(propInfoKvp => propInfoKvp.Value.Any());
                if (rowKeyModificationProperties.Any())
                {
                    var rowKeyModificationProperty = rowKeyModificationProperties.First();
                    var rowKeyProperty = rowKeyModificationProperty.Key;
                    var rowKeyGenerator = rowKeyModificationProperty.Value.First();
                    var rowKeyValue = rowKeyGenerator.GenerateRowKey(this.Entity, rowKeyProperty);
                    return rowKeyValue;
                }

                {
                    var attributesKvp = properties
                        .Where(propInfo => propInfo.ContainsCustomAttribute<StoragePropertyAttribute>())
                        .Select(propInfo => propInfo.PairWithKey(propInfo.GetCustomAttribute<StoragePropertyAttribute>()))
                        .ToArray();
                    var rowKeyProperty = attributesKvp
                        .First<KeyValuePair<StoragePropertyAttribute, MemberInfo>, MemberInfo>(
                            (attr, next) =>
                            {
                                if (attr.Key.IsRowKey)
                                    return attr.Value;
                                return next();
                            },
                            () => throw new Exception("Entity does not contain row key attribute"));

                    var rowKeyValue = rowKeyProperty.GetValue(Entity);
                    if (rowKeyValue.GetType().IsSubClassOfGeneric(typeof(IReferenceable)))
                    {
                        var rowKeyRef = rowKeyValue as IReferenceable;
                        var rowKeyRefString = rowKeyRef.id.AsRowKey();
                        return rowKeyRefString;
                    }
                    var rowKeyString = ((Guid)rowKeyValue).AsRowKey();
                    return rowKeyString;
                }
            }
            set
            {
                var x = value.GetType();
            }
        }

        public string PartitionKey
        {
            get
            {
                var partitionModificationProperties = typeof(EntityType)
                    .GetMembers()
                    .Where(propInfo => propInfo.ContainsAttributeInterface<IModifyAzureStorageTablePartitionKey>())
                    .Select(propInfo => propInfo.GetAttributesInterface<IModifyAzureStorageTablePartitionKey>().PairWithKey(propInfo))
                    .Where(propInfoKvp => propInfoKvp.Value.Any());
                if (!partitionModificationProperties.Any())
                    throw new Exception("Entity does not contain partition key attribute");

                var partitionModificationProperty = partitionModificationProperties.First();
                var partitionKeyProperty = partitionModificationProperty.Key;
                var partitionKeyGenerator = partitionModificationProperty.Value.First();

                var partitionKey = partitionKeyGenerator.GeneratePartitionKey(this.RowKey, this.Entity, partitionKeyProperty);
                return partitionKey;
            }
            set
            {
                var x = value.GetType();

            }
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

        public void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            this.Entity = CreateEntityInstance(properties);
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

        internal static ITableEntity Create<TEntity>(TEntity entity, string etag = "*")
        {
            var creatableEntity = new TableEntity<TEntity>();
            creatableEntity.Entity = entity;
            creatableEntity.ETag = etag;
            return creatableEntity;
        }
    }
}
