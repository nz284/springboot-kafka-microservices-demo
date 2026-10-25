package demo;

import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import java.net.URI;
import java.util.*;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/orders")
public class OrderApi {
  private final Orders orders;

  public OrderApi(Orders orders) {
    this.orders = orders;
  }

  public record Request(
      @NotNull UUID orderId,
      @NotBlank @Pattern(regexp = "[A-Za-z0-9_-]{1,64}") String sku,
      @Min(1) @Max(1000) int quantity) {}

  @PostMapping
  public ResponseEntity<Map<String, Object>> create(@Valid @RequestBody Request r) {
    return ResponseEntity.accepted()
        .location(URI.create("/api/orders/" + r.orderId()))
        .body(orders.create(r.orderId(), r.sku(), r.quantity()));
  }

  @GetMapping("/{id}")
  public Map<String, Object> get(@PathVariable UUID id) {
    return orders.get(id);
  }
}
