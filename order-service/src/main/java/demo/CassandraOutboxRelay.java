package demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.TimeUnit;
import org.slf4j.*;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class CassandraOutboxRelay {
  private static final Logger log = LoggerFactory.getLogger(CassandraOutboxRelay.class);
  private final CassandraOrderStore store;
  private final KafkaTemplate<String, String> kafka;
  private final ObjectMapper json;
  private final MeterRegistry meters;
  private int nextShard;

  public CassandraOutboxRelay(
      CassandraOrderStore store,
      KafkaTemplate<String, String> kafka,
      ObjectMapper json,
      MeterRegistry meters) {
    this.store = store;
    this.kafka = kafka;
    this.json = json;
    this.meters = meters;
  }

  @Scheduled(
      fixedDelayString = "${demo.outbox.delay:1000}",
      initialDelayString = "${demo.outbox.initial-delay:0}")
  public void publish() {
    try {
      int published = 0;
      for (int i = 0; i < CassandraOrderStore.SHARDS; i++) {
        int shard = nextShard;
        nextShard = (nextShard + 1) % CassandraOrderStore.SHARDS;
        for (OrderRecord e : store.pending(shard, 20 - published)) {
          Event event = new Event(e.id(), e.sku(), e.quantity(), "PENDING");
          kafka
              .send(Messaging.REQUESTS, e.id().toString(), json.writeValueAsString(event))
              .get(10, TimeUnit.SECONDS);
          // A crash before deletion can replay the event. Inventory deduplicates by order ID.
          store.markPublished(e.id());
          meters.counter("demo.outbox.published").increment();
          if (++published == 20) return;
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      failure(e);
    } catch (Exception e) {
      failure(e);
    }
  }

  private void failure(Exception e) {
    meters.counter("demo.outbox.failures").increment();
    log.warn("Cassandra Outbox remains pending; retry next poll", e);
  }
}
