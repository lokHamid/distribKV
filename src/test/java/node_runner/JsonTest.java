package node_runner;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JsonTest {

    @Test
    void roundTripsScalars() {
        assertEquals("null", Json.write(null));
        assertEquals("true", Json.write(true));
        assertEquals("42", Json.write(42));
        assertEquals("3.5", Json.write(3.5));
        assertEquals("\"hi\"", Json.write("hi"));
    }

    @Test
    void roundTripsNestedStructures() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("name", "distrokv");
        map.put("ok", true);
        map.put("count", 3);
        map.put("pi", 2.5);
        map.put("note", null);
        map.put("list", List.of("a", 1, true));
        map.put("nested", Map.of("k", "v"));

        String json = Json.write(map);
        Object parsed = Json.parse(json);
        assertEquals(map, parsed);
    }

    @Test
    void escapesSpecialCharacters() {
        String json = Json.write("a\"b\\c\nd\te");
        Object parsed = Json.parse(json);
        assertEquals("a\"b\\c\nd\te", parsed);
    }

    @Test
    void parsesExplicitJson() {
        Object parsed = Json.parse("{\"role\":\"LEADER\",\"term\":7,\"leaderId\":null}");
        Map<String, Object> map = Json.parseObject("{\"role\":\"LEADER\",\"term\":7,\"leaderId\":null}");
        assertEquals("LEADER", map.get("role"));
        assertEquals(7, map.get("term"));
        assertNull(map.get("leaderId"));
    }

    @Test
    void rejectsMalformed() {
        assertThrows(IllegalArgumentException.class, () -> Json.parse("{"));
        assertThrows(IllegalArgumentException.class, () -> Json.parse("{\"a\":}"));
    }
}
