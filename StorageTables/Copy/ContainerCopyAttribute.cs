using System;

namespace BlackBarLabs.Persistence.Azure.StorageTables.Backups
{
    public class ContainerCopyAttribute : Attribute
    {
        public string Group { get; set; }
        public string Name { get; set; }
        public bool Enabled { get; set; }

        public ContainerCopyAttribute(string group, string name, bool enabled = true)
        {
            Group = group;
            Name = name;
            Enabled = enabled;
        }
    }
}
