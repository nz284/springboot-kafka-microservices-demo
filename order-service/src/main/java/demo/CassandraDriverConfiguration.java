package demo;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import org.springframework.boot.autoconfigure.cassandra.DriverConfigLoaderBuilderCustomizer;
import org.springframework.context.annotation.*;

@Configuration
public class CassandraDriverConfiguration {
  @Bean
  DriverConfigLoaderBuilderCustomizer consistency() {
    return builder ->
        builder
            .withString(DefaultDriverOption.REQUEST_CONSISTENCY, "LOCAL_QUORUM")
            .withString(DefaultDriverOption.REQUEST_SERIAL_CONSISTENCY, "LOCAL_SERIAL");
  }
}
