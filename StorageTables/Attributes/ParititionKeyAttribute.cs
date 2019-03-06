using BlackBarLabs.Persistence.Azure;
using EastFive.Linq.Expressions;
using EastFive.Reflection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace EastFive.Persistence.Azure.StorageTables
{
    public class ParititionKeyAttribute : Attribute,
        IModifyAzureStorageTablePartitionKey
    {
        public string GeneratePartitionKey(string rowKey, object value, MemberInfo memberInfo)
        {
            var partitionValue = memberInfo.GetValue(value);
            return (string)partitionValue;
        }

        public EntityType ParsePartitionKey<EntityType>(EntityType entity, string value, MemberInfo memberInfo)
        {
            if (memberInfo.GetPropertyOrFieldType().IsAssignableFrom(typeof(string)))
                memberInfo.SetValue(ref entity, value);

            // otherwise, discard ...?
            return entity;
        }
    }

    public class StandardParititionKeyAttribute : Attribute,
       IModifyAzureStorageTablePartitionKey
    {
        public string GeneratePartitionKey(string rowKey, object value, MemberInfo memberInfo)
        {
            return rowKey.GeneratePartitionKey();
        }

        public EntityType ParsePartitionKey<EntityType>(EntityType entity, string value, MemberInfo memberInfo)
        {
            // discard since generated from id
            return entity;
        }
    }
}
