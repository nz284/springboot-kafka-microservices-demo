package demo;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.UUID;

public record OrderRecord(
    UUID id,
    String sku,
    int quantity,
    String status,
    @JsonProperty("created_at") Instant createdAt) {}
