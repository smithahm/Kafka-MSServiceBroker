package com.sql.sbbroker;

import java.io.File;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;

public class App {
  public static void main(String[] args) throws ClassNotFoundException, SQLException, JAXBException {
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
    Connection con = DriverManager.getConnection(
        "jdbc:sqlserver://localhost:1434;databaseName=testdb;instanceName=SQLEXPRESS", "sa", "Smith1234");

    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "test");
    props.put("enable.auto.commit", "true");
    props.put("auto.offset.reset", "earliest");
    props.put("auto.commit.interval.ms", "1000");
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "com.sql.sbbroker.JsonPOJODeserializer");

    KafkaConsumer<String, Data> kafkaConsumer = new KafkaConsumer<String, Data>(props);
    kafkaConsumer.subscribe(Arrays.asList("key-based-data"));

    while (true) {
      ConsumerRecords<String, Data> records = kafkaConsumer.poll(100);
      for (ConsumerRecord<String, Data> record : records) {

        // Create jaxbContext
        JAXBContext jaxbContext = JAXBContext.newInstance(Data.class);
        // Getting Marshaller object
        Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
        jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        jaxbMarshaller.setProperty(Marshaller.JAXB_FRAGMENT, true);
        // Writing onto the file "test.xml"
        jaxbMarshaller.marshal(record.value(), new File("test.xml"));
        StringWriter sw = new StringWriter();
        jaxbMarshaller.marshal(record.value(), sw);

        String result = sw.toString();

        Statement stmt = con.createStatement();
        String sql = "DECLARE @InitDlgHandle UNIQUEIDENTIFIER " + "DECLARE @RequestMessage VARCHAR(1000) "
            + "BEGIN DIALOG @InitDlgHandle" + " FROM SERVICE " + "[//SBTest/SBSample/SBInitiatorService] "
            + " TO SERVICE" + " '//SBTest/SBSample/SBTargetService' " + "ON CONTRACT "
            + "[//SBTest/SBSample/SBContract] " + " WITH ENCRYPTION=OFF;" + "SELECT @RequestMessage = '" + result + "';"
            + "SEND ON CONVERSATION @InitDlgHandle " + "MESSAGE TYPE " + "[//SBTest/SBSample/RequestMessage] "
            + "(@RequestMessage);" + "SELECT @RequestMessage AS SentRequestMessage;"
            + "END CONVERSATION @InitDlgHandle; ";

        System.out.println(sql);
        ResultSet rs = stmt.executeQuery(sql);
        if (rs.next()) {
          System.out.println("wrote to SB queue");
        }
      }

    }

  }
}

// To ignore root name
@XmlRootElement(name = "Data")
@JsonRootName(value = "Data")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE, creatorVisibility = JsonAutoDetect.Visibility.NONE)
class Data {

  private String Entity;
  private String Key;
  private SBDataValue Value;

  /* Add Constructor */
  public Data() {
  }

  @com.fasterxml.jackson.annotation.JsonProperty("Entity")
  public String getEntity() {
    return Entity;
  }

  @XmlElement(name = "Entity")
  public void setEntity(String Entity) {
    this.Entity = Entity;
  }

  @JsonProperty("Key")
  public String getKey() {
    return Key;
  }

  @XmlElement(name = "Key")
  public void setKey(String key) {
    this.Key = key;
  }

  @JsonProperty("Value")
  public SBDataValue getValue() {
    return Value;
  }

  @XmlElement(name = "Value")
  public void setValue(SBDataValue val) {
    this.Value = val;
  }

}

class SBDataValue {

  private int Buy;
  private int Sell;

  public SBDataValue() {
  }

  @JsonProperty("Buy")
  public int getBuy() {
    return Buy;
  }

  @XmlElement(name = "Buy")
  public void setBuy(int buy) {
    this.Buy = buy;
  }

  @JsonProperty("Sell")
  public int getSell() {
    return Sell;
  }

  @XmlElement(name = "Sell")
  public void setSell(int sell) {
    this.Sell = sell;
  }

}