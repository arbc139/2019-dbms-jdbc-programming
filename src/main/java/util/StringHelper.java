package util;

public class StringHelper {
  public static String escapeDoubleQuote(String str) {
    return String.format("\"%s\"", str);
  }

  public static String escapeSingleQuote(String str) {
    return String.format("'%s'", str);
  }
}
