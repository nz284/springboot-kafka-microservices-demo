package demo;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.*;
import org.springframework.web.server.ResponseStatusException;

class OrdersTest {
  CassandraOrderStore store = mock(CassandraOrderStore.class);
  ObjectMapper json = new ObjectMapper();
  SimpleMeterRegistry meters = new SimpleMeterRegistry();
  Orders orders = new Orders(store, json, meters);
  UUID id = UUID.randomUUID();
  OrderRecord pending = new OrderRecord(id, "DEMO-SKU", 2, "PENDING", Instant.now());

  @BeforeEach
  void row() {
    when(store.find(id)).thenReturn(Optional.of(pending));
  }

  @Test
  void newOrderUsesAtomicStoreAndCountsOnce() {
    when(store.insertWithEvent(id, "DEMO-SKU", 2)).thenReturn(true);
    assertEquals(pending, orders.create(id, "DEMO-SKU", 2));
    verify(store).insertWithEvent(id, "DEMO-SKU", 2);
    assertEquals(1, meters.counter("demo.orders.created").count());
  }

  @Test
  void duplicateDoesNotCountAnotherOrder() {
    orders.create(id, "DEMO-SKU", 2);
    assertEquals(0, meters.counter("demo.orders.created").count());
  }

  @Test
  void conflictingRequestReturns409() {
    assertEquals(
        409,
        assertThrows(ResponseStatusException.class, () -> orders.create(id, "DEMO-SKU", 3))
            .getStatusCode()
            .value());
  }

  @Test
  void invalidResultDoesNotAccessStore() {
    assertThrows(IllegalArgumentException.class, () -> orders.result("{}"));
    verify(store, never()).complete(any(), any());
  }

  @Test
  void resultUsesConditionalStateTransition() throws Exception {
    when(store.complete(id, "CONFIRMED")).thenReturn(true);
    orders.result(json.writeValueAsString(new Event(id, "DEMO-SKU", 2, "CONFIRMED")));
    assertEquals(1, meters.counter("demo.orders.completed", "status", "CONFIRMED").count());
  }

  @Test
  void duplicateTerminalResultIsHarmless() throws Exception {
    when(store.find(id))
        .thenReturn(
            Optional.of(new OrderRecord(id, "DEMO-SKU", 2, "CONFIRMED", pending.createdAt())));
    orders.result(json.writeValueAsString(new Event(id, "DEMO-SKU", 2, "CONFIRMED")));
    assertEquals(0, meters.counter("demo.orders.completed", "status", "CONFIRMED").count());
  }

  @Test
  void contradictoryTerminalResultIsRejected() throws Exception {
    when(store.find(id))
        .thenReturn(
            Optional.of(new OrderRecord(id, "DEMO-SKU", 2, "CONFIRMED", pending.createdAt())));
    assertThrows(
        IllegalArgumentException.class,
        () -> orders.result(json.writeValueAsString(new Event(id, "DEMO-SKU", 2, "REJECTED"))));
  }
}
