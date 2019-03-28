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
    public class LastModifiedAttribute : Attribute,
        IModifyAzureStorageTableLastModified
    {
        public DateTimeOffset GenerateLastModified(object value, MemberInfo memberInfo)
        {
            var lastModifiedValue = memberInfo.GetValue(value);
            var lastModifiedType = memberInfo.GetMemberType();
            if (typeof(DateTimeOffset).IsAssignableFrom(lastModifiedType))
            {
                var dateTimeValue = (DateTimeOffset)lastModifiedValue;
                return dateTimeValue;
            }
            var message = $"`{this.GetType().FullName}` Cannot determine last modified from type `{lastModifiedType.FullName}` on `{memberInfo.DeclaringType.FullName}..{memberInfo.Name}`";
            throw new NotImplementedException(message);
        }

        public EntityType ParseLastModfied<EntityType>(EntityType entity, DateTimeOffset value, MemberInfo memberInfo)
        {
            var memberType = memberInfo.GetMemberType();
            if (memberType.IsAssignableFrom(typeof(DateTimeOffset)))
            {
                memberInfo.SetValue(ref entity, value);
                return entity;
            }
            if (memberType.IsAssignableFrom(typeof(DateTime)))
            {
                var dateTime = value.UtcDateTime;
                memberInfo.SetValue(ref entity, dateTime);
                return entity;
            }
            var message = $"`{this.GetType().FullName}` Cannot determine row key from type `{memberType.FullName}` on `{memberInfo.DeclaringType.FullName}..{memberInfo.Name}`";
            throw new NotImplementedException(message);
        }
    }
}
