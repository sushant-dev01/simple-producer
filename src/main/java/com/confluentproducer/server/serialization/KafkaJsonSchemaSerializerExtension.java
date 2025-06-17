package com.confluentproducer.server.serialization;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.time.ZoneId;
import java.util.TimeZone;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Headers;
import org.openapitools.jackson.nullable.JsonNullableModule;

@Slf4j
public class KafkaJsonSchemaSerializerExtension extends KafkaJsonSchemaSerializer<Object> {

  private static final JsonSchema EMPTY_SCHEMA = new JsonSchema("{}");
  public static final String SCHEMA_REGISTRY_ERROR_MESSAGE = "Error retrieving latest version of schema for topic: ";
  private final ObjectMapper mapper = new ObjectMapper();
  public static final ZoneId CENTRAL_EUROPEAN_ZONE_ID = ZoneId.of("Europe/Berlin");

  public KafkaJsonSchemaSerializerExtension() {
    super();
    mapper.registerModule(new JavaTimeModule());
    mapper.registerModule(new JsonNullableModule());
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.setTimeZone(TimeZone.getTimeZone(CENTRAL_EUROPEAN_ZONE_ID.getId()));
  }

  /**
   * Serializes the given data object to bytes using Confluent JSON schema serialization.
   *
   * @param topic   the Kafka topic the data will be written to
   * @param headers the Kafka message headers
   * @param data    the object to be serialized
   * @return the serialized data as a byte array
   * @throws SerializationException        if there's an error during serialization
   * @throws InvalidConfigurationException if schema registry is not configured
   */
  @Override
  @SneakyThrows
  public byte[] serialize(String topic, Headers headers, Object data) {
    byte[] allBytes = toByteArray(data);
    JsonNode jsonNode = mapper.readTree(allBytes);
    return serializeImpl(getSubjectName(topic, isKey, jsonNode, EMPTY_SCHEMA), topic, headers,
        jsonNode, EMPTY_SCHEMA);
  }

  @Override
  protected byte[] serializeImpl(String subject, String topic, Headers headers, Object object, JsonSchema schema)
      throws SerializationException, InvalidConfigurationException {
    if (schemaRegistry == null) {
      throw new InvalidConfigurationException("SchemaRegistryClient not found.");
    }

    try {
      if (validate) {
        schema = (JsonSchema) lookupLatestVersion(subject, schema, false);
        validateJson(object, schema);
      }
      return mapper.writeValueAsBytes(object);
    } catch (InterruptedIOException e) {
      throw new TimeoutException("Error serializing JSON message", e);
    } catch (IOException | RuntimeException e) {
      throw new SerializationException("Error serializing JSON message", e);
    } catch (RestClientException e) {
      throw toKafkaException(e, SCHEMA_REGISTRY_ERROR_MESSAGE + topic);
    } finally {
      postOp(object);
    }
  }

  private byte[] toByteArray(Object data) throws JsonProcessingException {
    return mapper.writeValueAsBytes(data);
  }
}

