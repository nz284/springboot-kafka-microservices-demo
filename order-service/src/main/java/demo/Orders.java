package demo;

import static org.springframework.http.HttpStatus.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

@Service
public class Orders {
  private final JdbcTemplate db;
  private final Outbox outbox;
  private final ObjectMapper json;
  private final MeterRegistry meters;

  public Orders(JdbcTemplate db, Outbox outbox, ObjectMapper json, MeterRegistry meters) {
    this.db = db;
    this.outbox = outbox;
    this.json = json;
    this.meters = meters;
  }

  @Transactional
  public Map<String, Object> create(UUID id, String sku, int quantity) {
    int inserted =
        db.update(
            "insert into orders(id,sku,quantity,status) values (?,?,?,'PENDING') on conflict(id) do"
                + " nothing",
            id,
            sku,
            quantity);
    var order = get(id);
    if (!order.get("sku").equals(sku) || ((Number) order.get("quantity")).intValue() != quantity)
      throw new ResponseStatusException(CONFLICT, "Order ID reused with different payload");
    if (inserted == 1) {
      outbox.add(Messaging.REQUESTS, new Event(id, sku, quantity, "PENDING"));
      meters.counter("demo.orders.created").increment();
    }
    return order;
  }

  public Map<String, Object> get(UUID id) {
    var rows = db.queryForList("select * from orders where id=?", id);
    if (rows.isEmpty()) throw new ResponseStatusException(NOT_FOUND, "Order not found");
    return rows.getFirst();
  }

  @KafkaListener(topics = Messaging.RESULTS)
  @Transactional
  public void result(String payload) {
    Event e = Outbox.decode(json, payload);
    if (!Set.of("CONFIRMED", "REJECTED").contains(e.status()))
      throw new IllegalArgumentException("Invalid status");
    var order = get(e.orderId());
    if (!order.get("sku").equals(e.sku())
        || ((Number) order.get("quantity")).intValue() != e.quantity())
      throw new IllegalArgumentException("Result mismatches order");
    int changed =
        db.update(
            "update orders set status=? where id=? and status='PENDING'", e.status(), e.orderId());
    if (changed == 1) meters.counter("demo.orders.completed", "status", e.status()).increment();
  }
}
