// node_runner/Json.java
package node_runner;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Minimal, dependency-free JSON encoder/decoder for the dashboard wire
 * contract. Supported values: {@code null}, {@code Boolean}, {@code Number},
 * {@code String}, {@code Map<String,Object>} and {@code List<Object>}. Values
 * are produced as {@code Map}/{@code List}/{@code String}/{@code Number}/
 * {@code Boolean}/{@code null} by {@link #parse(String)}.
 *
 * <p>Both DistribKV and the JavaFX dashboard use this shape, so the contract is
 * plain JSON with no shared library dependency.
 */
public final class Json {

    private Json() {
    }

    // ------------------------------------------------------------------ encode

    public static String write(Object value) {
        StringBuilder sb = new StringBuilder();
        write(sb, value);
        return sb.toString();
    }

    private static void write(StringBuilder sb, Object value) {
        if (value == null) {
            sb.append("null");
        } else if (value instanceof String s) {
            writeString(sb, s);
        } else if (value instanceof Boolean || value instanceof Integer
                || value instanceof Long || value instanceof Double || value instanceof Float
                || value instanceof Short || value instanceof Byte) {
            sb.append(value);
        } else if (value instanceof Map<?, ?> map) {
            sb.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> e : map.entrySet()) {
                if (!first) {
                    sb.append(',');
                }
                writeString(sb, String.valueOf(e.getKey()));
                sb.append(':');
                write(sb, e.getValue());
                first = false;
            }
            sb.append('}');
        } else if (value instanceof Iterable<?> list) {
            sb.append('[');
            boolean first = true;
            for (Object item : list) {
                if (!first) {
                    sb.append(',');
                }
                write(sb, item);
                first = false;
            }
            sb.append(']');
        } else {
            writeString(sb, value.toString());
        }
    }

    private static void writeString(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
    }

    // ------------------------------------------------------------------ decode

    public static Object parse(String text) {
        Parser p = new Parser(text);
        Object value = p.parseValue();
        p.skipWhitespace();
        if (!p.atEnd()) {
            throw new IllegalArgumentException("Unexpected trailing content at offset " + p.pos);
        }
        return value;
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> parseObject(String text) {
        Object value = parse(text);
        if (!(value instanceof Map)) {
            throw new IllegalArgumentException("Expected a JSON object");
        }
        return (Map<String, Object>) value;
    }

    private static final class Parser {
        private final String s;
        private int pos = 0;

        Parser(String s) {
            this.s = s;
        }

        boolean atEnd() {
            return pos >= s.length();
        }

        void skipWhitespace() {
            while (pos < s.length()) {
                char c = s.charAt(pos);
                if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                    pos++;
                } else {
                    break;
                }
            }
        }

        Object parseValue() {
            skipWhitespace();
            if (atEnd()) {
                throw error("unexpected end");
            }
            char c = s.charAt(pos);
            return switch (c) {
                case '{' -> parseObjectValue();
                case '[' -> parseArray();
                case '"' -> parseString();
                case 't' -> parseKeyword("true", Boolean.TRUE);
                case 'f' -> parseKeyword("false", Boolean.FALSE);
                case 'n' -> parseKeyword("null", null);
                default -> {
                    if (c == '-' || (c >= '0' && c <= '9')) {
                        yield parseNumber();
                    }
                    throw error("unexpected character '" + c + "'");
                }
            };
        }

        private Object parseObjectValue() {
            expect('{');
            Map<String, Object> map = new LinkedHashMap<>();
            skipWhitespace();
            if (atEnd()) {
                throw error("unexpected end of object");
            }
            if (peek() == '}') {
                pos++;
                return map;
            }
            while (true) {
                skipWhitespace();
                if (atEnd()) {
                    throw error("unexpected end of object");
                }
                String key = parseString();
                skipWhitespace();
                expect(':');
                Object val = parseValue();
                map.put(key, val);
                skipWhitespace();
                if (atEnd()) {
                    throw error("unexpected end of object");
                }
                char c = next();
                if (c == '}') {
                    break;
                }
                if (c != ',') {
                    throw error("expected ',' or '}'");
                }
            }
            return map;
        }

        private List<Object> parseArray() {
            expect('[');
            List<Object> list = new ArrayList<>();
            skipWhitespace();
            if (atEnd()) {
                throw error("unexpected end of array");
            }
            if (peek() == ']') {
                pos++;
                return list;
            }
            while (true) {
                list.add(parseValue());
                skipWhitespace();
                if (atEnd()) {
                    throw error("unexpected end of array");
                }
                char c = next();
                if (c == ']') {
                    break;
                }
                if (c != ',') {
                    throw error("expected ',' or ']'");
                }
            }
            return list;
        }

        private String parseString() {
            expect('"');
            StringBuilder sb = new StringBuilder();
            while (true) {
                char c = next();
                if (c == '"') {
                    break;
                }
                if (c == '\\') {
                    char esc = next();
                    switch (esc) {
                        case '"' -> sb.append('"');
                        case '\\' -> sb.append('\\');
                        case '/' -> sb.append('/');
                        case 'b' -> sb.append('\b');
                        case 'f' -> sb.append('\f');
                        case 'n' -> sb.append('\n');
                        case 'r' -> sb.append('\r');
                        case 't' -> sb.append('\t');
                        case 'u' -> {
                            String hex = s.substring(pos, pos + 4);
                            pos += 4;
                            sb.append((char) Integer.parseInt(hex, 16));
                        }
                        default -> throw error("bad escape '\\" + esc + "'");
                    }
                } else {
                    sb.append(c);
                }
            }
            return sb.toString();
        }

        private Object parseKeyword(String kw, Object value) {
            if (!s.startsWith(kw, pos)) {
                throw error("expected '" + kw + "'");
            }
            pos += kw.length();
            return value;
        }

        private Object parseNumber() {
            int start = pos;
            if (peek() == '-') {
                pos++;
            }
            while (pos < s.length() && isDigit(s.charAt(pos))) {
                pos++;
            }
            boolean frac = false;
            if (pos < s.length() && s.charAt(pos) == '.') {
                frac = true;
                pos++;
                while (pos < s.length() && isDigit(s.charAt(pos))) {
                    pos++;
                }
            }
            if (pos < s.length() && (s.charAt(pos) == 'e' || s.charAt(pos) == 'E')) {
                frac = true;
                pos++;
                if (pos < s.length() && (s.charAt(pos) == '+' || s.charAt(pos) == '-')) {
                    pos++;
                }
                while (pos < s.length() && isDigit(s.charAt(pos))) {
                    pos++;
                }
            }
            String num = s.substring(start, pos);
            try {
                if (frac) {
                    return Double.parseDouble(num);
                }
                long l = Long.parseLong(num);
                return (l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE) ? (Object) (int) l : (Object) l;
            } catch (NumberFormatException e) {
                throw error("invalid number '" + num + "'");
            }
        }

        private boolean isDigit(char c) {
            return c >= '0' && c <= '9';
        }

        private char peek() {
            return s.charAt(pos);
        }

        private char next() {
            return s.charAt(pos++);
        }

        private void expect(char c) {
            skipWhitespace();
            if (atEnd() || s.charAt(pos) != c) {
                throw error("expected '" + c + "'");
            }
            pos++;
        }

        private IllegalArgumentException error(String msg) {
            return new IllegalArgumentException("JSON parse error at offset " + pos + ": " + msg);
        }
    }
}
