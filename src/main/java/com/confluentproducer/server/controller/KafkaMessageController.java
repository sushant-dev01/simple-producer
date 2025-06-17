package com.confluentproducer.server.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.openapitools.model.sdp.SdpServiceOrder;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Component
@RequestMapping("/api/kafka/test")
@Slf4j
@RequiredArgsConstructor
public class KafkaMessageController {

  private static final String DOMAIN = "som";
  private final ObjectMapper objectMapper;

  private final KafkaTemplate<String, Object> kafkaTemplate;

  @PostMapping("/send/{eventType}")
  public ResponseEntity<String> sendMessage(@PathVariable String eventType, @RequestBody String payload) {
    try {
      Object typedPayload = parsePayload(eventType, payload);

      String eventId = UUID.randomUUID().toString();
      String eventTime = OffsetDateTime.now().toString();
      String topic = "dev.tmfapi.serviceOrderingManagement.v4.serviceOrder.notificationEvent";
      String correlationId = ((SdpServiceOrder) typedPayload).getId();

      ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(topic,
          ((SdpServiceOrder) typedPayload).getId(), typedPayload);

      addHeaders(producerRecord, eventId, eventTime, eventType, correlationId);

//      log.info("Sending message to topic {} \n {}", topic, producerRecord);
      kafkaTemplate.send(producerRecord);
      kafkaTemplate.flush();

      return ResponseEntity.ok()
          .body(String.format("Message sent with eventId: %s, eventType: %s", eventId, eventType));

    } catch (Exception ex) {
      log.error("Failed to send message", ex);
      return ResponseEntity.badRequest().body("Failed to send message: " + ex.getMessage());
    }
  }

  private static final Map<String, String> EVENT_TYPE_MAPPING = new HashMap<>();

  {
    EVENT_TYPE_MAPPING.put("serviceOrderStateChangeEvent", "ServiceOrderStateChangeEvent");
    EVENT_TYPE_MAPPING.put("serviceOrderAttributeValueChangeEvent", "ServiceOrderAttributeValueChangeEvent");
  }

  private void addHeaders(ProducerRecord<String, Object> producerRecord, String eventId, String eventTime,
      String eventType, String correlationId) {
    producerRecord.headers().add(new RecordHeader("eventId", eventId.getBytes(StandardCharsets.UTF_8)));
    producerRecord.headers().add(new RecordHeader("eventTime", eventTime.getBytes(StandardCharsets.UTF_8)));
    producerRecord.headers()
        .add(new RecordHeader("eventType", EVENT_TYPE_MAPPING.get(eventType).getBytes(StandardCharsets.UTF_8)));
    producerRecord.headers().add(new RecordHeader("domain", DOMAIN.getBytes(StandardCharsets.UTF_8)));
    producerRecord.headers().add("correlationId", correlationId.getBytes(StandardCharsets.UTF_8));
  }

  private Object parsePayload(String eventType, String payload) {
    try {
      Map<String, Object> payloadMap = objectMapper.readValue(payload, Map.class);

      return objectMapper.convertValue(payloadMap, SdpServiceOrder.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to parse payload for event type: " + eventType, e);
    }
  }
}
