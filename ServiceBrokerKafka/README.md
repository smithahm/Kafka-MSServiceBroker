This demo application is to push messages from Service Broker Queue to Kafka Topics in real-time written in C# and Java (KafkaStreamAPI folder)

Requirements:
C#, 
.net framework4.5.2, 
Kafka 0.10.2.0, 
ZooKeeper, 
MS SQL server with Service Broker enabled. 

Service Broker to Kafka topic "service-broker-data" using Kafka Producer API
 1. Service Broker needs to be enabled in SQL server to put some messages to the target queue.
 2. Have Kafka and ZooKeeper installed in your system. Start both.
 3. Create a kafka topic named 'service-broker-data' using Kafka.
 4. To install Kafka package run the following command in the NuGet Package Manager Console:
       Install-Package Confluent.Kafka -Version 0.11.0
 5. Start this windows service application in Visual Studio
 6. Put some messages in Service broker queue after application is started.
    Sample XML data to be put in Service Broker :
```xml
<Data>
<Entity>orders</Entity>
<Key>new</Key>
<Value>
<Buy>100</Buy>
<Sell>50</Sell>
</Value>
</Data> 
```
 7. Go to kafka installation directory and see the messages being loaded to Kafka topic using below command  ::: 
      ./bin/windows/kafka-console-consumer.bat --zookeeper localhost:2181 --from-beginning --topic service-broker-data
