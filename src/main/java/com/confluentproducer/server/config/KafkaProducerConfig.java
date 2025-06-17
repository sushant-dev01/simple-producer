package com.confluentproducer.server.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.handler.logging.LogLevel;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.DefaultSslBundleRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.http.codec.json.Jackson2JsonDecoder;
import org.springframework.http.codec.json.Jackson2JsonEncoder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;
import reactor.netty.transport.logging.AdvancedByteBufFormat;

@Configuration
@Getter
@Slf4j
public class KafkaProducerConfig {

//  private final ObjectMapper objectMapper;

  @Value("${weg.kafka.producer.topics.l2bsa-service-order}")
  private String l2bsaServiceOrder;

//  KafkaProducerConfig(ObjectMapper objectMapper) {
//    this.objectMapper = objectMapper;
//  }
//
  @Bean
  public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
    return new KafkaTemplate<>(producerFactory);
  }

  @Bean
  public ProducerFactory<String, Object> producerFactory(KafkaProperties kafkaProperties) {
    Map<String, Object> producerProps = new HashMap<>(
        kafkaProperties.getProducer().buildProperties(new DefaultSslBundleRegistry()));
    log.info("Kafka Producer properties: {}", producerProps);
    return new DefaultKafkaProducerFactory<>(producerProps);
  }

  @Bean
  public WebClient webClient(WebClient.Builder webClientBuilder, ObjectMapper mapper) {
    final HttpClient httpClient = HttpClient
        .create()
        .wiretap("reactor.netty.http.client.HttpClient",
            LogLevel.DEBUG, AdvancedByteBufFormat.TEXTUAL);

    return webClientBuilder
        .clientConnector(new ReactorClientHttpConnector(httpClient))
        .codecs(clientDefaultCodecsConfigure -> {
          clientDefaultCodecsConfigure.defaultCodecs().jackson2JsonEncoder(
              new Jackson2JsonEncoder(mapper, MediaType.APPLICATION_JSON));
          clientDefaultCodecsConfigure.defaultCodecs().jackson2JsonDecoder(
              new Jackson2JsonDecoder(mapper, MediaType.APPLICATION_JSON));
        })
        .build();
  }
}
