using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace BlackBarLabs.Persistence.Azure.StorageTables
{
    internal static class InternalExtensions
    {
        internal static void SetFieldOrProperty(this object document, bool value, FieldInfo fieldInfo, PropertyInfo propertyInfo)
        {
            if (fieldInfo != null) fieldInfo.SetValue(document, false);
            else propertyInfo?.SetValue(document, false);
        }
    }
}
