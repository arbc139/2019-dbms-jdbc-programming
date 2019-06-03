package util;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class TxtCsvParser {
  public static class KeyOrderMap {
    public KeyOrderMap(List<String> keyOrder, Map<String, String> map) {
      this.keyOrder = keyOrder;
      this.map = map;
    }

    public List<String> keyOrder;
    public Map<String, String> map;
  }

  public void open(String path) throws FileNotFoundException {
    reader = new BufferedReader(
        new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_8)
    );
  }

  public void close() {
    try {
      reader.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Map<String, String> parseTxt() throws RuntimeException {
    Map<String, String> result = new HashMap<String, String>();
    String line;
    try {
      while ((line = reader.readLine()) != null) {
        String[] entry = line.split(":");
        if (entry.length != 2) {
          throw new RuntimeException("[Error][util.TxtCsvParser] Invalid file format!");
        }
        String key = entry[0].trim();
        String value = entry[1].trim();
        result.put(key, value);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  public KeyOrderMap parseOrderedTxt() throws RuntimeException {
    List<String> keyOrder = new ArrayList<String>();
    Map<String, String> result = new HashMap<String, String>();
    String line;
    try {
      while ((line = reader.readLine()) != null) {
        String[] entry = line.split(":");
        if (entry.length != 2) {
          throw new RuntimeException("[Error][util.TxtCsvParser] Invalid file format!");
        }
        String key = entry[0].trim();
        String value = entry[1].trim();
        result.put(key, value);
        keyOrder.add(key);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return new KeyOrderMap(keyOrder, result);
  }

  public List<Map<String, String>> parseCsv() {
    List<Map<String, String>> result = new ArrayList<Map<String, String>>();
    String[] header;
    try {
      String line;
      line = reader.readLine();
      if (line == null) {
        throw new RuntimeException("[Error][util.TxtCsvParser][CSV] no line in file");
      }
      header = line.split(",");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    reader.lines().forEachOrdered(line -> {
      String[] rawEntry = line.split(",");
      if (rawEntry.length != header.length) {
        throw new RuntimeException("[Error][util.TxtCsvParser] Invalid file format!");
      }
      Map<String, String> entry = new HashMap<String, String>();
      for (int i = 0; i < rawEntry.length; ++i) {
        entry.put(header[i], rawEntry[i]);
      }
      result.add(entry);
    });
    return result;
  }

  private BufferedReader reader;
}
