package util;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class TxtCsvWriter {
  public static String makeCsvRow(List<String> cols) {
    return String.join(",", cols);
  }

  public void open(String path) throws IOException {
    writer = new BufferedWriter(new OutputStreamWriter(
        new FileOutputStream(path), StandardCharsets.UTF_8
    ));
  }

  public void close() {
    try {
      writer.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void writeCsv(List<String> csvRows) {
    try {
      for (String row : csvRows) {
        writer.write(row);
        writer.newLine();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  BufferedWriter writer;
}
