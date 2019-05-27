package util;

public class StringHelper {
  public static String escape(String str) {
    return String.format("\"%s\"", str);
  }
}
