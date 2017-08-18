This demo application is the Java part of ServiceBrokerToKafka code.

Requirements:
Kafka 0.10.2.0, 
ZooKeeper, 
MS SQL server with Service Broker enabled, 
Java8, 
Eclipse/IntelliJ.


To write data from Kafka topic "service-broker-data" to another topic "key-based-data" using Kafka Stream API.
This demo is based on the value in the XML tag <Key>

1. Create a topic named "key-based-data" in Kafka.
2. Run the java application App.Java which has Kafka Stream code.
3. Records based on the key value given in Stream Builder will be displayed in the "key-based-data". Also the record displayed will have the total sum of BUY value of previous record