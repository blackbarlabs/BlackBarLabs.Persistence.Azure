using System;
using System.Collections.Generic;

namespace BlackBarLabs.Persistence.Azure.StorageTables.RelationshipDocuments
{
	[Serializable]
    internal class ParentDocument : DocumentTemplate<Guid>
    {
        public ParentDocument() {}

        public ParentDocument(Guid id)
            : base(id)
        {
		}

        public List<IDocument> OrderedListOfEntities { get; set; }
        public Guid NextDocument { get; set; }
    }
}
