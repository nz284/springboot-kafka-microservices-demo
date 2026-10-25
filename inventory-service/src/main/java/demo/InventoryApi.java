package demo;

import static org.springframework.http.HttpStatus.NOT_FOUND;

import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/inventory")
public class InventoryApi {
  private final JdbcTemplate db;

  public InventoryApi(JdbcTemplate db) {
    this.db = db;
  }

  @GetMapping("/{sku}")
  public Map<String, Object> get(@PathVariable String sku) {
    var rows = db.queryForList("select * from stock where sku=?", sku);
    if (rows.isEmpty()) throw new ResponseStatusException(NOT_FOUND, "SKU not found");
    return rows.getFirst();
  }
}
