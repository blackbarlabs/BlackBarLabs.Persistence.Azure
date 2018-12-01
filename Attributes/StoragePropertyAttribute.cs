using EastFive.Collections.Generic;
using EastFive.Extensions;
using EastFive.Reflection;
using EastFive.Serialization;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace EastFive.Persistence
{
    public interface IPersistInAzureStorageTables
    {
        KeyValuePair<string, EntityProperty>[] ConvertValue(object value, MemberInfo memberInfo);
        void PopulateValue(object value, MemberInfo memberInfo, IDictionary<string, EntityProperty> entities);
    }

    public class StoragePropertyAttribute : Attribute, IPersistInAzureStorageTables
    {
        public string Name { get; set; }
        public bool IsRowKey { get; set; }
        public Type ReferenceType { get; set; }
        public string ReferenceProperty { get; set; }

        public virtual KeyValuePair<string, EntityProperty>[] ConvertValue(object value, MemberInfo memberInfo)
        {
            var propertyName = this.Name.IsNullOrWhiteSpace(
                () => memberInfo.Name,
                (text) => text);

            return ConvertValue(value, propertyName);
        }

        public static KeyValuePair<string, EntityProperty>[] ConvertValue(object value, string propertyName)
        {
            KeyValuePair<string, EntityProperty>[] ReturnProperty(EntityProperty property)
            {
                var kvp = property.PairWithKey(propertyName);
                return (kvp.AsArray());
            }

            if (value.IsDefaultOrNull())
                return new KeyValuePair<string, EntityProperty>[] { };

            if (typeof(string).IsInstanceOfType(value))
            {
                var stringValue = value as string;
                var ep = new EntityProperty(stringValue);
                return ReturnProperty(ep);
            }
            if (typeof(bool).IsInstanceOfType(value))
            {
                var boolValue = (bool)value;
                var ep = new EntityProperty(boolValue);
                return ReturnProperty(ep);
            }
            if (typeof(Guid).IsInstanceOfType(value))
            {
                var guidValue = (Guid)value;
                var ep = new EntityProperty(guidValue);
                return ReturnProperty(ep);
            }
            if (typeof(Guid[]).IsInstanceOfType(value))
            {
                var guidsValue = value as Guid[];
                var ep = new EntityProperty(guidsValue.ToByteArrayOfGuids());
                return ReturnProperty(ep);
            }
            if (typeof(Guid[]).IsInstanceOfType(value))
            {
                var guidsValue = value as Guid[];
                var ep = new EntityProperty(guidsValue.ToByteArrayOfGuids());
                return ReturnProperty(ep);
            }
            if (typeof(Type).IsInstanceOfType(value))
            {
                var typeValue = (value as Type);
                var typeString = typeValue.AssemblyQualifiedName;
                var ep = new EntityProperty(typeString);
                return ReturnProperty(ep);
            }
            if (typeof(IReferenceable).IsAssignableFrom(value.GetType()))
            {
                var refValue = value as IReferenceable;
                var guidValue = refValue.id;
                var ep = new EntityProperty(guidValue);
                return ReturnProperty(ep);
            }
            if (typeof(IReferences).IsAssignableFrom(value.GetType()))
            {
                var refValue = value as IReferences;
                var guidValues = refValue.ids;
                var ep = new EntityProperty(guidValues.ToByteArrayOfGuids());
                return ReturnProperty(ep);
            }
            if (value.GetType().IsSubClassOfGeneric(typeof(IDictionary<,>)))
            {
                var valueType = value.GetType();
                var keysType = valueType.GenericTypeArguments[0];
                var valuesType = valueType.GenericTypeArguments[1];
                var kvps = value
                    .DictionaryKeyValuePairs();
                var keyValues = kvps
                    .SelectKeys()
                    .Cast(keysType);
                var valueValues = kvps
                    .SelectValues()
                    .Cast(valuesType);
                return ConvertValue(keyValues, $"{propertyName}__keys")
                    .Concat(ConvertValue(valueValues, $"{propertyName}__values"))
                    .ToArray();
            }
            if (value.GetType().IsArray)
            {
                var valueType = ((System.Collections.IEnumerable)value)
                    .Cast<object>()
                    .ToArray()
                    .Select(
                        item =>
                        {
                            var epValues = ConvertValue(item, "DISCARD");
                            if (!epValues.Any())
                                throw new Exception($"Cannot serialize array of `{item.GetType().FullName}`.");
                            var epValue = epValues.First().Value;
                            switch (epValue.PropertyType)
                            {
                                case EdmType.Binary:
                                    return epValue.BinaryValue;
                                case EdmType.Boolean:
                                    return epValue.BooleanValue.Value ?
                                        new byte[] { 1 } : new byte[] { 0 };
                                case EdmType.Double:
                                    return BitConverter.GetBytes(epValue.DoubleValue.Value);
                                case EdmType.Guid:
                                    return epValue.GuidValue.Value.ToByteArray();
                                case EdmType.Int32:
                                    return BitConverter.GetBytes(epValue.Int32Value.Value);
                                case EdmType.Int64:
                                    return BitConverter.GetBytes(epValue.Int32Value.Value);
                                case EdmType.String:
                                    return epValue.StringValue.GetBytes();
                            }
                            return new byte[] { };
                        })
                    .ToByteArray();
                return new EntityProperty(valueType).PairWithKey(propertyName).AsArray();
            }

            return new KeyValuePair<string, EntityProperty>[] { };
        }

        public void PopulateValue(object value, MemberInfo memberInfo, IDictionary<string, EntityProperty> entities)
        {
            throw new NotImplementedException();
        }
    }
}
