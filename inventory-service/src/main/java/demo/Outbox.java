package demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

@Component
public class Outbox {
  private static final Logger log = LoggerFactory.getLogger(Outbox.class);
  private final JdbcTemplate db;
  private final ObjectMapper json;
  private final KafkaTemplate<String, String> kafka;
  private final TransactionTemplate tx;
  private final MeterRegistry meters;

  public Outbox(
      JdbcTemplate db,
      ObjectMapper json,
      KafkaTemplate<String, String> kafka,
      TransactionTemplate tx,
      MeterRegistry meters) {
    this.db = db;
    this.json = json;
    this.kafka = kafka;
    this.tx = tx;
    this.meters = meters;
  }

  // Must be called inside the business transaction using the same datasource.
  public void add(String topic, Event event) {
    try {
      db.update(
          "insert into outbox(id,topic,event_key,payload) values (?,?,?,?)",
          UUID.randomUUID(),
          topic,
          event.orderId().toString(),
          json.writeValueAsString(event));
    } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Scheduled(fixedDelayString = "${demo.outbox.delay:1000}")
  public void publish() {
    try {
      tx.executeWithoutResult(
          s -> {
            var rows =
                db.queryForList(
                    "select * from outbox order by created_at,id limit 20 for update skip locked");
            for (var row : rows) {
              try {
                kafka
                    .send(
                        (String) row.get("topic"),
                        (String) row.get("event_key"),
                        (String) row.get("payload"))
                    .get(10, TimeUnit.SECONDS);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
              } catch (Exception e) {
                throw new IllegalStateException(e);
              }
              db.update("delete from outbox where id=?", row.get("id"));
              meters.counter("demo.outbox.published").increment();
            }
          });
    } catch (Exception e) {
      meters.counter("demo.outbox.failures").increment();
      log.warn("Outbox pending; retry next poll", e);
    }
  }
}
