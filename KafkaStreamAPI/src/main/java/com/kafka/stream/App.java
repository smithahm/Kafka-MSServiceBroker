package com.kafka.stream;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;

public class App {

  public static void main(final String[] args) throws Exception {
    final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
    final Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "sb-stream-example");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
    streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    final Serde<String> stringSerde = Serdes.String();
    final KStreamBuilder builder = new KStreamBuilder();

    // define DataSerde
    Map<String, Object> serdeProps = new HashMap<>();
    final Serializer<Data> dataSerializer = new JsonPOJOSerializer<>();
    serdeProps.put("JsonPOJOClass", Data.class);
    dataSerializer.configure(serdeProps, false);

    final Deserializer<Data> dataDeserializer = new JsonPOJODeserializer<>();
    serdeProps.put("JsonPOJOClass", Data.class);
    dataDeserializer.configure(serdeProps, false);
    final Serde<Data> dataSerde = Serdes.serdeFrom(dataSerializer, dataDeserializer);

    final KStream<String, Data> textLines = builder.stream(stringSerde, dataSerde, "service-broker-data");

    // Group by Key needs the custom serilaization used else it will use default
    // kafka serdes and throws error. this groups based on the first key seen.
    /*
     * textLines.groupByKey(stringSerde, dataSerde).reduce((v1, v2) -> sum(v1,
     * v2), "sum-test").to(stringSerde, dataSerde, topicName);
     */

    // Grouping the records based on the value of the key. "old"
    textLines.groupBy((key, value) -> "old", stringSerde, dataSerde).reduce((v1, v2) -> sum(v1, v2), "sb-test")
        .to(stringSerde, dataSerde, "key-based-data");

    final KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
    streams.cleanUp();
    streams.start();
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

  private static Data sum(Data v1, Data v2) {
    v1.getValue().setBuy(v1.getValue().getBuy() + v2.getValue().getBuy());
    System.out.println("data in reduce funtion" + v1.getValue().getBuy());
    return v1;
  }

}

// To ignore root name
@JsonRootName(value = "Data")
// To avoid duplicate data in the deserialized output
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE, creatorVisibility = JsonAutoDetect.Visibility.NONE)
class Data {

  private String Entity;
  private String Key;
  private SBDataValue Value;

  /* Add Constructor */
  public Data() {
  }

  @JsonProperty("Entity")
  public String getEntity() {
    return Entity;
  }

  public void setEntity(String Entity) {
    this.Entity = Entity;
  }

  @JsonProperty("Key")
  public String getKey() {
    return Key;
  }

  public void setKey(String key) {
    this.Key = key;
  }

  @JsonProperty("Value")
  public SBDataValue getValue() {
    return Value;
  }

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

  public void setBuy(int buy) {
    this.Buy = buy;
  }

  @JsonProperty("Sell")
  public int getSell() {
    return Sell;
  }

  public void setSell(int sell) {
    this.Sell = sell;
  }

}