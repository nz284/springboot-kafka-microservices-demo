package demo;

import java.time.Duration;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.*;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.*;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class Messaging {
  public static final String REQUESTS = "orders.created.v1", RESULTS = "inventory.results.v1";

  @Bean
  NewTopic requests() {
    return topic(REQUESTS);
  }

  @Bean
  NewTopic results() {
    return topic(RESULTS);
  }

  @Bean
  NewTopic requestDlt() {
    return topic(REQUESTS + ".DLT");
  }

  @Bean
  NewTopic resultDlt() {
    return topic(RESULTS + ".DLT");
  }

  private NewTopic topic(String name) {
    return TopicBuilder.name(name).partitions(3).replicas(1).build();
  }

  @Bean
  DefaultErrorHandler errorHandler(KafkaTemplate<String, String> template) {
    var recoverer =
        new DeadLetterPublishingRecoverer(
            template, (r, e) -> new TopicPartition(r.topic() + ".DLT", r.partition()));
    recoverer.setFailIfSendResultIsError(true);
    recoverer.setWaitForSendResultTimeout(Duration.ofSeconds(10));
    var handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2L));
    handler.addNotRetryableExceptions(IllegalArgumentException.class);
    return handler;
  }
}
