package demo;

import com.fasterxml.jackson.databind.ObjectMapper;

public final class Events {
  private Events() {}

  public static Event decode(ObjectMapper json, String payload) {
    try {
      Event event = json.readValue(payload, Event.class);
      event.validate();
      return event;
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid event payload", e);
    }
  }
}
