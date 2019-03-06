using BlackBarLabs.Persistence.Azure.StorageTables;
using EastFive.Linq.Expressions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace EastFive.Persistence.Azure.StorageTables
{
    public class RowKeyAttribute : Attribute,
        IModifyAzureStorageTableRowKey
    {
        public virtual string GenerateRowKey(object value, MemberInfo memberInfo)
        {
            var partitionValue = memberInfo.GetValue(value);
            var propertyValueType = memberInfo.GetMemberType();
            if (typeof(Guid).IsAssignableFrom(propertyValueType))
            {
                var guidValue = (Guid)partitionValue;
                return guidValue.AsRowKey();
            }
            if (typeof(IReferenceable).IsAssignableFrom(propertyValueType))
            {
                var refValue = (IReferenceable)partitionValue;
                return refValue.id.AsRowKey();
            }
            if (typeof(string).IsAssignableFrom(propertyValueType))
            {
                var stringValue = (string)partitionValue;
                return stringValue;
            }
            var message = $"`{this.GetType().FullName}` Cannot determine row key from type `{propertyValueType.FullName}` on `{memberInfo.DeclaringType.FullName}..{memberInfo.Name}`";
            throw new NotImplementedException(message);
        }

        void IModifyAzureStorageTableRowKey.ParseRowKey<EntityType>(EntityType entity, string value, MemberInfo memberInfo)
        {
            var memberType = memberInfo.GetMemberType();
            if (typeof(Guid).IsAssignableFrom(memberType))
            {
                if (Guid.TryParse(value, out Guid guidValue))
                {
                    memberInfo.SetValue(ref entity, guidValue);
                    return;
                }
            }
            if (memberType.IsSubClassOfGeneric(typeof(IRef<>)))
            {
                if (Guid.TryParse(value, out Guid guidValue))
                {
                    var refdType = memberType.GenericTypeArguments.First();
                    var genericType = typeof(Ref<>).MakeGenericType(refdType);
                    var refValue = Activator.CreateInstance(genericType, new object[] { guidValue });
                    memberInfo.SetValue(ref entity, refValue);
                    return;
                }
            }
            if (memberType.IsAssignableFrom(typeof(string)))
            {
                memberInfo.SetValue(ref entity, value);
                return;
            }
            var message = $"`{this.GetType().FullName}` Cannot determine row key from type `{memberType.FullName}` on `{memberInfo.DeclaringType.FullName}..{memberInfo.Name}`";
            throw new NotImplementedException(message);
        }
    }
}
