
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Linq;
using System.Diagnostics;
using System.Data.SqlClient;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using System.Xml;
using System.IO;
using Newtonsoft.Json.Linq;

namespace ServiceBrokerToKafka


{
    class InboundMessageProcessor
    {
        public static void ProcessMessage(byte[] message, Dictionary<string, object> config)
        {
            string topicName = "from-sb";
            XmlDocument doc = new XmlDocument();
            doc.LoadXml(System.Text.Encoding.Default.GetString(message));
            String json = Newtonsoft.Json.JsonConvert.SerializeXmlNode(doc);
            var key = JObject.Parse(json)["Data"]["Key"];
            {
                using (var producer = new Producer<string, string>(config, new StringSerializer(Encoding.UTF8), new StringSerializer(Encoding.UTF8)))
                {
                    var deliveryReport = producer.ProduceAsync(topicName, key.ToString(), json);

                    deliveryReport.ContinueWith(task =>
                    {
                        Console.WriteLine($"Partition: {task.Result.Partition}, Offset: {task.Result.Offset}");
                    });
                    producer.Flush(TimeSpan.FromSeconds(10));
                }
                return;
            }
        }

        public static void SaveFailedMessage(byte[] message, SqlConnection con, Exception errorInfo)
        {
            Console.Write(System.Text.Encoding.Default.GetString(message));
            return;
        }
    }
}

