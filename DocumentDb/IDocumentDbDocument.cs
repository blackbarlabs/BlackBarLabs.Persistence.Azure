using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BlackBarLabs.Persistence.Azure.DocumentDb
{
    public interface IDocumentDbDocument
    {
        Guid Id { get; set; }
    }

}
