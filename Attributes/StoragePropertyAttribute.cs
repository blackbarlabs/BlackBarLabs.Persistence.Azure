using BlackBarLabs.Persistence.Azure;
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
        string Name { get; }
        KeyValuePair<string, EntityProperty>[] ConvertValue(object value, MemberInfo memberInfo);
        object GetMemberValue(MemberInfo memberInfo, IDictionary<string, EntityProperty> values);
    }

    public interface IModifyAzureStorageTablePartitionKey
    {
        string GeneratePartitionKey(string rowKey, object value, MemberInfo memberInfo);
    }

    public class StoragePropertyAttribute : Attribute, 
        IPersistInAzureStorageTables, IModifyAzureStorageTablePartitionKey
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

        public virtual object GetMemberValue(MemberInfo memberInfo, IDictionary<string, EntityProperty> values)
        {
            var propertyName = this.Name.IsNullOrWhiteSpace(
                () => memberInfo.Name,
                (text) => text);

            var type = memberInfo.GetPropertyOrFieldType();

            if (!values.ContainsKey(propertyName))
                return BindEmptyValue(type, values,
                    (convertedValue) => convertedValue,
                    () =>
                    {
                        var exceptionText = $"Could not create empty value for {memberInfo.DeclaringType.FullName}..{memberInfo.Name}[{type.FullName}]" +
                            "Please override StoragePropertyAttribute's BindEmptyValue for property:";
                        throw new Exception(exceptionText);
                    });

            var value = values[propertyName];
            return BindEntityProperty(type, value,
                (convertedValue) => convertedValue,
                () =>
                {
                    var exceptionText = $"Could not cast `{{0}}` to {type.FullName}." +
                        "Please override StoragePropertyAttribute's BindEntityProperty for property:" +
                        $"{memberInfo.DeclaringType.FullName}..{memberInfo.Name}";
                    if (value.PropertyType == EdmType.Binary)
                    {
                        var valueString = value.BinaryValue.Select(x => x.ToString("X2")).Join("");
                        throw new Exception(String.Format(exceptionText, valueString));
                    }
                    throw new Exception(String.Format(exceptionText, value.StringValue));
                });
        }

        protected virtual TResult BindEmptyValue<TResult>(Type type,
                IDictionary<string, EntityProperty> allValues,
            Func<object, TResult> onBound,
            Func<TResult> onFailedToBind)
        {
            if(type.IsSubClassOfGeneric(typeof(IDictionary<,>)))
            {
                // TODO: Actually map values
                var keyType = type.GenericTypeArguments[0];
                var valueType = type.GenericTypeArguments[1];
                var instantiatableType = typeof(Dictionary<,>)
                    .MakeGenericType(keyType, valueType);
                var refOpt = Activator.CreateInstance(instantiatableType, new object[] { });
                return onBound(refOpt);
            }

            if (type.IsAssignableFrom(typeof(Guid)))
                return onBound(default(Guid));

            if (type.IsAssignableFrom(typeof(Guid[])))
                return onBound(new Guid[] { });

            if (type.IsSubClassOfGeneric(typeof(IRef<>)))
            {
                var resourceType = type.GenericTypeArguments.First();
                var instance = resourceType.GetDefault();
                return onBound(instance);
            }

            if (type.IsSubClassOfGeneric(typeof(IRefOptional<>)))
            {
                var resourceType = type.GenericTypeArguments.First();
                var instantiatableType = typeof(EastFive.RefOptional<>)
                    .MakeGenericType(resourceType);
                var refOpt = Activator.CreateInstance(instantiatableType, new object[] { });
                return onBound(refOpt);
            }

            if (type.IsSubClassOfGeneric(typeof(IRefs<>)))
            {
                var resourceType = type.GenericTypeArguments.First();
                var instantiatableType = typeof(EastFive.Azure.Persistence.Refs<>).MakeGenericType(resourceType);
                var instance = Activator.CreateInstance(instantiatableType, new object[] { new Guid[] { } });
                return onBound(instance);
            }

            if (type.IsAssignableFrom(typeof(string)))
                return onBound(default(string));

            if (type.IsSubClassOfGeneric(typeof(Nullable<>)))
            {
                var resourceType = type.GenericTypeArguments.First();
                var instantiatableType = typeof(Nullable<>).MakeGenericType(resourceType);
                var instance = Activator.CreateInstance(instantiatableType, new object[] { });
                return onBound(instance);
            }

            return onFailedToBind();
        }

        protected virtual TResult BindEntityProperty<TResult>(Type type, EntityProperty value,
            Func<object, TResult> onBound,
            Func<TResult> onFailedToBind)
        {
            if (type.IsAssignableFrom(typeof(Guid)))
                return onBound(value.GuidValue);

            if (type.IsAssignableFrom(typeof(Guid[])))
            {
                var guidsValue = value.BinaryValue.ToGuidsFromByteArray();
                return onBound(guidsValue);
            }

            object IRefInstance(Guid guidValue)
            {
                var resourceType = type.GenericTypeArguments.First();
                var instantiatableType = typeof(EastFive.Azure.Persistence.Ref<>).MakeGenericType(resourceType);
                var instance = Activator.CreateInstance(instantiatableType, new object[] { guidValue });
                return instance;
            }

            if (type.IsSubClassOfGeneric(typeof(IRef<>)))
            {
                var guidValue = value.GuidValue.Value;
                var instance = IRefInstance(guidValue);
                //var resourceType = type.GenericTypeArguments.First();
                //var instantiatableType = typeof(EastFive.Azure.Persistence.Ref<>).MakeGenericType(resourceType);
                //var instance = Activator.CreateInstance(instantiatableType, new object[] { guidValue });
                return onBound(instance);
            }

            if (type.IsSubClassOfGeneric(typeof(IRefOptional<>)))
            {
                var guidValueMaybe = value.PropertyType == EdmType.Binary?
                    default(Guid?)
                    :
                    value.GuidValue;
                var resourceType = type.GenericTypeArguments.First();
                var instantiatableType = typeof(EastFive.RefOptional<>)
                    .MakeGenericType(resourceType);
                if (!guidValueMaybe.HasValue)
                {
                    var refOpt = Activator.CreateInstance(instantiatableType, new object[] { });
                    return onBound(refOpt);
                }
                var guidValue = guidValueMaybe.Value;
                var refValue = IRefInstance(guidValue);
                var instance = Activator.CreateInstance(instantiatableType, new object[] { refValue });
                return onBound(instance);
            }

            if (type.IsSubClassOfGeneric(typeof(IRefs<>)))
            {
                var guidValues = value.BinaryValue.ToGuidsFromByteArray();
                var resourceType = type.GenericTypeArguments.First();
                var instantiatableType = typeof(EastFive.Azure.Persistence.Refs<>).MakeGenericType(resourceType);
                var instance = Activator.CreateInstance(instantiatableType, new object[] { guidValues });
                return onBound(instance);
            }

            if (type.IsAssignableFrom(typeof(string)))
                return onBound(value.StringValue);

            if (type.IsAssignableFrom(typeof(bool)))
                return onBound(value.BooleanValue);

            return onFailedToBind();
        }

        public string GeneratePartitionKey(string rowKey, object value, MemberInfo memberInfo)
        {
            return rowKey.GeneratePartitionKey();
        }
    }
}
