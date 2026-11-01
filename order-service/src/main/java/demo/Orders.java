package demo;

import static org.springframework.http.HttpStatus.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.*;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
public class Orders {
  private final CassandraOrderStore store;
  private final ObjectMapper json;
  private final MeterRegistry meters;

  public Orders(CassandraOrderStore store, ObjectMapper json, MeterRegistry meters) {
    this.store = store;
    this.json = json;
    this.meters = meters;
  }

  public OrderRecord create(UUID id, String sku, int quantity) {
    boolean inserted = store.insertWithEvent(id, sku, quantity);
    OrderRecord order = get(id);
    if (!order.sku().equals(sku) || order.quantity() != quantity)
      throw new ResponseStatusException(CONFLICT, "Order ID reused with different payload");
    if (inserted) meters.counter("demo.orders.created").increment();
    return order;
  }

  public OrderRecord get(UUID id) {
    return store
        .find(id)
        .orElseThrow(() -> new ResponseStatusException(NOT_FOUND, "Order not found"));
  }

  @KafkaListener(topics = Messaging.RESULTS)
  public void result(String payload) {
    Event e = Events.decode(json, payload);
    if (!Set.of("CONFIRMED", "REJECTED").contains(e.status()))
      throw new IllegalArgumentException("Invalid result status");
    OrderRecord order = get(e.orderId());
    if (!order.sku().equals(e.sku()) || order.quantity() != e.quantity())
      throw new IllegalArgumentException("Result mismatches order");
    if (store.complete(e.orderId(), e.status()))
      meters.counter("demo.orders.completed", "status", e.status()).increment();
    else if (!get(e.orderId()).status().equals(e.status()))
      throw new IllegalArgumentException("Conflicting terminal result");
  }
}
