package demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class Inventory {
  private final JdbcTemplate db;
  private final Outbox outbox;
  private final ObjectMapper json;
  private final MeterRegistry meters;

  public Inventory(JdbcTemplate db, Outbox outbox, ObjectMapper json, MeterRegistry meters) {
    this.db = db;
    this.outbox = outbox;
    this.json = json;
    this.meters = meters;
  }

  @KafkaListener(topics = Messaging.REQUESTS)
  @Transactional
  public void reserve(String payload) {
    Event e = Outbox.decode(json, payload);
    if (!"PENDING".equals(e.status())) throw new IllegalArgumentException("Invalid status");
    // Unique constraint serializes duplicates; marker rolls back if later work fails.
    int first =
        db.update(
            "insert into reservations(order_id,sku,quantity,status) values (?,?,?,'PENDING') on"
                + " conflict(order_id) do nothing",
            e.orderId(),
            e.sku(),
            e.quantity());
    if (first == 0) return;
    // Atomic conditional update prevents overselling across concurrent transactions.
    int updated =
        db.update(
            "update stock set available=available-? where sku=? and available>=?",
            e.quantity(),
            e.sku(),
            e.quantity());
    String status = updated == 1 ? "CONFIRMED" : "REJECTED";
    db.update("update reservations set status=? where order_id=?", status, e.orderId());
    outbox.add(Messaging.RESULTS, new Event(e.orderId(), e.sku(), e.quantity(), status));
    meters.counter("demo.inventory.reservations", "status", status).increment();
  }
}
