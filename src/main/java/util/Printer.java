package util;

import java.util.Map;

public class Printer {
  public static void printMap(Map<String, String> map) {
    for (Map.Entry<String, String> entry : map.entrySet()) {
      System.out.println(String.format("[KEY] %s, [VALUE] %s", entry.getKey(), entry.getValue()));
    }
  }
}
