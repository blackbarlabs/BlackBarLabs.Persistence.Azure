using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EastFive.Persistence.Azure.StorageTables
{
    public static class EDMExtensions
    {
        public static object ToObjectFromEdmTypeByteArray(this EdmType type, byte[] values)
        {
            switch (type)
            {
                case EdmType.Binary:
                    {
                        return values;
                    }
                case EdmType.Boolean:
                    {
                        if (!values.Any())
                            return default(bool?);
                        return values[0] != 0;
                    }
                case EdmType.DateTime:
                    {
                        if (!values.Any())
                            return default(DateTime?);
                        var ticks = BitConverter.ToInt64(values, 0);
                        return new DateTime(ticks);
                    }
                case EdmType.Double:
                    {
                        if (!values.Any())
                            return default(double?);
                        var value = BitConverter.ToDouble(values, 0);
                        return value;
                    }
                case EdmType.Guid:
                    {
                        if (!values.Any())
                            return default(Guid?);
                        var value = new Guid(values);
                        return value;
                    }
                case EdmType.Int32:
                    {
                        if (!values.Any())
                            return default(int?);
                        var value = BitConverter.ToInt32(values, 0);
                        return value;
                    }
                case EdmType.Int64:
                    {
                        if (!values.Any())
                            return default(long?);
                        var value = BitConverter.ToInt64(values, 0);
                        return value;
                    }
                case EdmType.String:
                    {
                        if (!values.Any())
                            return default(string);

                        return Encoding.UTF8.GetString(values);
                    }
            }
            throw new Exception($"Unrecognized EdmType {type}");
        }

    }
}
