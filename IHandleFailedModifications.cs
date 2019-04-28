using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace EastFive.Persistence.Azure
{
    public interface IHandleFailedModifications<TResult>
    {
        bool DoesMatchMember(MemberInfo[] membersWithFailures);

        TResult ModificationFailure(MemberInfo[] membersWithFailures);
    }
}
