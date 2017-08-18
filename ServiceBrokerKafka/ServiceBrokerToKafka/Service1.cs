
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading;
using System.Xml.Linq;
using System.Data.SqlClient;
using System.Transactions;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace ServiceBrokerToKafka
{
    public partial class Service1 : ServiceBase
    {

        class QueueListenerConfig
        {
            public string QueueName { get; set; }
            public int Threads { get; set; }
            public bool EnlistMessageProcessor { get; set; }
            public Action<byte[], Dictionary<string, object>> MessageProcessor { get; set; }
            public Action<byte[], SqlConnection, Exception> FailedMessageProcessor { get; set; }
            public string ConnectionString { get; set; }
        }

        static List<QueueListenerConfig> QueueSettings = new List<QueueListenerConfig>();
        static List<Thread> Listeners = new List<Thread>();

        static bool stopping = false;

        public Service1()
        {
            InitializeComponent();
        }

        public void StartService()
        {
            OnStart(null);
        }
        public void StopService()
        {
            OnStop();
        }
        protected override void OnStart(string[] args)
        {
            System.Diagnostics.Debugger.Launch();
            //load the config
            var l = new QueueListenerConfig();
            l.QueueName = "SBTargetQueue";  //The name of the service broker target queue
            l.Threads = 1;
            l.EnlistMessageProcessor = false;
            l.MessageProcessor = InboundMessageProcessor.ProcessMessage;  //Wire up the message processors
            l.FailedMessageProcessor = InboundMessageProcessor.SaveFailedMessage;
            l.ConnectionString = "Data Source=smallika;Initial Catalog=testdb;Integrated Security=True;MultipleActiveResultSets=True;";
            QueueSettings.Add(l);



            foreach (var q in QueueSettings)
            {
                for (int i = 0; i < q.Threads; i++)
                {
                    Thread listenerThread = new Thread(ListenerThreadProc);
                    listenerThread.Name = "Listener Thread " + i.ToString() + " for " + q.QueueName;
                    listenerThread.IsBackground = false;
                    Listeners.Add(listenerThread);

                    listenerThread.Start(q);
                    Trace.WriteLine("Started thread " + listenerThread.Name);
                }
            }

        }

        public static void ListenerThreadProc(object queueListenerConfig)
        {
            //Kafka configurations
            string brokerList = "localhost:9092";
            var kafkaConfig = new Dictionary<string, object> { { "bootstrap.servers", brokerList } };
            QueueListenerConfig config = (QueueListenerConfig)queueListenerConfig;

            while (!stopping)
            {
                TransactionOptions to = new TransactionOptions();
                to.IsolationLevel = System.Transactions.IsolationLevel.ReadCommitted;
                to.Timeout = TimeSpan.MaxValue;

                CommittableTransaction tran = new CommittableTransaction(to);

                try
                {
                    using (var con = new SqlConnection(config.ConnectionString))
                    {
                        con.Open();
                        con.EnlistTransaction(tran);
                        byte[] message = ServiceBrokerUtils.GetMessage(config.QueueName, con, TimeSpan.FromSeconds(10));

                        if (message == null) //no message available
                        {
                            tran.Commit();
                            con.Close();
                            continue;
                        }

                        try
                        {
                            if (config.EnlistMessageProcessor)
                            {
                                using (var ts = new TransactionScope(tran))
                                {
                                    config.MessageProcessor(message, kafkaConfig);
                                    ts.Complete();
                                }
                            }
                            else
                            {
                                config.MessageProcessor(message, kafkaConfig);
                            }

                        }
                        catch (SqlException ex) //catch selected exceptions thrown by the MessageProcessor
                        {
                            config.FailedMessageProcessor(message, con, ex);
                        }

                        tran.Commit(); // the message processing succeeded or the FailedMessageProcessor ran so commit the RECEIVE
                        con.Close();

                    }
                }
                catch (SqlException ex)
                {
                    System.Diagnostics.Trace.Write("Error processing message from " + config.QueueName + ": " + ex.Message);
                    tran.Rollback();
                    tran.Dispose();
                    Thread.Sleep(1000);
                }
                ///catch any other non-fatal exceptions that should not stop the listener loop.
                catch (Exception ex)
                {
                    Trace.WriteLine("Unexpected Exception in Thread Proc for " + config.QueueName + ".  Thread Proc is exiting: " + ex.Message);
                    tran.Rollback();
                    tran.Dispose();
                    return;
                }

            }
        }

        protected override void OnStop()
        {
            stopping = true;
            foreach (var t in Listeners)
            {
                t.Join(20 + 1000);
            }

        }
    }
}
