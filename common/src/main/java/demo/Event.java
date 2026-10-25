package demo;

import java.util.UUID;

public record Event(UUID orderId, String sku, int quantity, String status) {
  public void validate() {
    if (orderId == null
        || sku == null
        || !sku.matches("[A-Za-z0-9_-]{1,64}")
        || quantity < 1
        || quantity > 1000) throw new IllegalArgumentException("Invalid event");
  }
}
