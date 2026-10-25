package demo;

import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.*;
import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.server.ResponseStatusException;

class OrdersTest {
  JdbcTemplate db = mock(JdbcTemplate.class);
  Outbox outbox = mock(Outbox.class);
  UUID id = UUID.randomUUID();
  Orders orders = new Orders(db, outbox, new ObjectMapper(), new SimpleMeterRegistry());

  @BeforeEach
  void row() {
    when(db.queryForList(anyString(), eq(id)))
        .thenReturn(List.of(Map.of("sku", "DEMO-SKU", "quantity", 2)));
  }

  @Test
  void newOrderQueuesEvent() {
    when(db.update(startsWith("insert into orders"), any(), any(), any())).thenReturn(1);
    orders.create(id, "DEMO-SKU", 2);
    verify(outbox).add(Messaging.REQUESTS, new Event(id, "DEMO-SKU", 2, "PENDING"));
  }

  @Test
  void sameRequestDoesNotEmitAgain() {
    orders.create(id, "DEMO-SKU", 2);
    verifyNoInteractions(outbox);
  }

  @Test
  void conflictingRequestRejected() {
    Assertions.assertThrows(ResponseStatusException.class, () -> orders.create(id, "DEMO-SKU", 3));
    verifyNoInteractions(outbox);
  }

  @Test
  void invalidResultRejected() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> orders.result("{}"));
  }
}
