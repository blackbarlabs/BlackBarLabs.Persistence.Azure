using System;

namespace BlackBarLabs.Persistence.Azure.StorageTables.RelationshipDocuments
{
	[Serializable]
    public class ChildDocument : DocumentTemplate<Guid>
    {
        public ChildDocument() {}

        public ChildDocument(Guid id)
            : base(id)
        {
		}

        public string OrderedListOfSharedEntities { get; set; }
        public string NextDocument { get; set; }
    }
}
