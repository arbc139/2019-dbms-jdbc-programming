package util;

import java.io.*;
import java.util.List;

public class TxtCsvWriter {
  public static String makeCsvRow(List<String> cols) {
    return String.join(",", cols);
  }

  public void open(String path) throws IOException {
    File file = new File(path);
    writer = new PrintWriter(new BufferedWriter(new FileWriter(file)));
  }

  public void close() {
    writer.close();
  }

  public void writeCsv(List<String> csvRows) {
    for (String row : csvRows) {
      writer.println(row);
    }
  }

  PrintWriter writer;
}
