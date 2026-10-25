package demo;

import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.UUID;
import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.JdbcTemplate;

class InventoryTest {
  JdbcTemplate db = mock(JdbcTemplate.class);
  Outbox outbox = mock(Outbox.class);
  ObjectMapper json = new ObjectMapper();
  Inventory service = new Inventory(db, outbox, json, new SimpleMeterRegistry());
  Event event = new Event(UUID.randomUUID(), "DEMO-SKU", 2, "PENDING");

  @Test
  void duplicateDoesNotReserveOrPublish() throws Exception {
    service.reserve(json.writeValueAsString(event));
    verifyNoInteractions(outbox);
    verify(db, never()).update(startsWith("update stock"), any(), any(), any());
  }

  @Test
  void invalidEventDoesNotTouchDatabase() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> service.reserve("{broken"));
    verifyNoInteractions(db, outbox);
  }

  @Test
  void insufficientStockIsBusinessRejection() throws Exception {
    when(db.update(startsWith("insert into reservations"), any(), any(), any())).thenReturn(1);
    service.reserve(json.writeValueAsString(event));
    verify(outbox).add(Messaging.RESULTS, new Event(event.orderId(), event.sku(), 2, "REJECTED"));
  }

  @Test
  void successfulReservationPublishesConfirmation() throws Exception {
    when(db.update(startsWith("insert into reservations"), any(), any(), any())).thenReturn(1);
    when(db.update(startsWith("update stock"), any(), any(), any())).thenReturn(1);
    service.reserve(json.writeValueAsString(event));
    verify(outbox).add(Messaging.RESULTS, new Event(event.orderId(), event.sku(), 2, "CONFIRMED"));
  }
}
