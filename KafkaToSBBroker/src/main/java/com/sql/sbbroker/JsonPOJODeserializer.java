package com.sql.sbbroker;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonPOJODeserializer implements Deserializer<Data> {

  public void configure(Map<String, ?> props, boolean isKey) {

  }

  public Data deserialize(String topic, byte[] data) {
    ObjectMapper mapper = new ObjectMapper();
    Data data1 = null;
    try {
      mapper.configure(DeserializationFeature.UNWRAP_ROOT_VALUE, true);
      data1 = mapper.readValue(data, Data.class);
    } catch (Exception e) {

      e.printStackTrace();
    }
    return data1;
  }

  public void close() {

  }

}
