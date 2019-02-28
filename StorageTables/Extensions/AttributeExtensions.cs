using BlackBarLabs.Extensions;
using EastFive.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace EastFive.Persistence
{
    public static class AttributeExtensions
    {
        public static IEnumerable<KeyValuePair<MemberInfo, IPersistInAzureStorageTables[]>> GetPersistenceAttributes(this Type type)
        {
            var memberQuery = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.FlattenHierarchy;
            var storageMembers = type
                .GetMembers(memberQuery)
                .Where(field => field.ContainsAttributeInterface<IPersistInAzureStorageTables>())
                .Select(field => field.PairWithValue(field.GetAttributesInterface<IPersistInAzureStorageTables>()));
            return storageMembers;
        }

        public static IEnumerable<KeyValuePair<MemberInfo, IPersistInAzureStorageTables>> StorageProperties(this Type entityType)
        {
            return entityType.GetPersistenceAttributes()
                .Select(
                    propInfoAttrsKvp =>
                    {
                        var propInfo = propInfoAttrsKvp.Key;
                        var attrs = propInfoAttrsKvp.Value;
                        if (attrs.Length > 1)
                        {
                            var propIdentifier = $"{propInfo.DeclaringType.FullName}__{propInfo.Name}";
                            var attributesInConflict = attrs.Select(a => a.GetType().FullName).Join(",");
                            throw new Exception($"{propIdentifier} has multiple IPersistInAzureStorageTables attributes:{attributesInConflict}.");
                        }
                        var attr = attrs.First() as IPersistInAzureStorageTables;
                        return attr.PairWithKey(propInfo);
                    });
        }
    }
}
