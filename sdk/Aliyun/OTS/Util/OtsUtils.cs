using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Aliyun.OTS.Aliyun.OTS.Util
{
    public static class OtsUtils
    {
        internal static string DetermineSystemArchitecture()
        {
            return (IntPtr.Size == 8) ? "x86_64" : "x86";
        }

        internal static string DetermineOsVersion()
        {
            try
            {
                var os = Environment.OSVersion;
                return "windows " + os.Version.Major + "." + os.Version.Minor;
            }
            catch (InvalidOperationException)
            {
                return "Unknown OSVersion";
            }
        }
    }
}
