using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceProcess;
using System.Text;

//Part of the code to connect to Service broker is copied from Microsoft developer network samples
namespace ServiceBrokerToKafka
{
    static class Program
    {
        static void Main(string[] args)
        {
            if (args.Length > 0 && args[0] == "/console")
            {
                System.Diagnostics.Trace.Listeners.Add(new System.Diagnostics.ConsoleTraceListener());
                var s = new Service1();
                s.StartService();
                Console.WriteLine("Started, hit any key to stop");
                Console.ReadKey();
                s.StopService();
                return;
            }

            ServiceBase[] ServicesToRun;
            ServicesToRun = new ServiceBase[]
            {
        new Service1()
            };
            ServiceBase.Run(ServicesToRun);
        }
    }
}
