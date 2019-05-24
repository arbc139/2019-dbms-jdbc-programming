
import javafx.util.Pair;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Schema {
  public static class Column {
    public Column(String name, String dataType, boolean isPrivateKey, boolean isNullable) {
      this.name = name;
      this.dataType = dataType;
      this.isPrivateKey = isPrivateKey;
      this.isNullable = isNullable;
    }

    public Column clone() {
      return new Column(name, dataType, isPrivateKey, isNullable);
    }

    public String toString() {
      return String.format(
          "[Column] name: '%s', dataType: '%s', isPrivateKey: '%b', isNullable: '%b'",
          name, dataType, isPrivateKey, isNullable);
    }

    String name;
    String dataType;
    boolean isPrivateKey;
    boolean isNullable;
  }

  public static class Builder {
    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    public Builder addColumn(String columnName, String columnDataType) {
      if (columns.containsKey(columnName)) {
        Column col = columns.get(columnName);
        col.name = columnName;
        col.dataType = columnDataType;
        return this;
      }
      columns.put(columnName, new Column(columnName, columnDataType, false, true));
      return this;
    }

    public Builder addPrivateKeyColumn(String columnName) {
      if (columns.containsKey(columnName)) {
        Column col = columns.get(columnName);
        col.isPrivateKey = true;
        return this;
      }
      columns.put(columnName, new Column(columnName, "", true, true));
      return this;
    }

    public Builder addNotNullColumn(String columnName) {
      if (columns.containsKey(columnName)) {
        Column col = columns.get(columnName);
        col.isNullable = false;
        return this;
      }
      columns.put(columnName, new Column(columnName, "", false, false));
      return this;
    }

    public Schema build() {
      return new Schema(this);
    }

    String name = "";
    Map<String, Column> columns = new HashMap<String, Column>();
  }

  private Schema(Schema.Builder builder) {
    this.name = builder.name;
    this.columns = new HashMap<String, Column>();
    for (Map.Entry<String, Column> entry : builder.columns.entrySet()) {
      columns.put(entry.getKey(), entry.getValue().clone());
    }
  }

  public static Schema parse(Map<String, String> tableDescription) {
    Schema.Builder builder = new Schema.Builder();

    Pattern namePattern = Pattern.compile("^Name$");
    Pattern columnPattern = Pattern.compile("^Column [0-9]+$");
    Pattern columnDataTypePattern = Pattern.compile("^Column [0-9]+ Data Type$");
    Pattern pkPattern = Pattern.compile("^PK$");
    Pattern notNullPattern = Pattern.compile("^Not NULL$");
    Map<Integer, Pair<String, String>> columnDataTypeMap = new HashMap<Integer, Pair<String, String>>();

    for (Map.Entry<String, String> entry : tableDescription.entrySet()) {
      Matcher nameMatcher = namePattern.matcher(entry.getKey());
      Matcher columnMatcher = columnPattern.matcher(entry.getKey());
      Matcher columnDataTypeMatcher = columnDataTypePattern.matcher(entry.getKey());
      Matcher pkMatcher = pkPattern.matcher(entry.getKey());
      Matcher notNullMatcher = notNullPattern.matcher(entry.getKey());

      if (nameMatcher.find()) {
        // Schema "Name"
        builder.setName(entry.getValue());
      } else if (columnMatcher.find()) {
        int columnNum = Integer.valueOf(entry.getKey().replaceAll("[^0-9]", ""));
        String rawColumnName = entry.getValue();
        if (columnDataTypeMap.containsKey(columnNum)) {
          String dataType = columnDataTypeMap.get(columnNum).getValue();
          columnDataTypeMap.put(columnNum, new Pair<String, String>(rawColumnName, dataType));
          continue;
        }
        columnDataTypeMap.put(columnNum, new Pair<String, String>(rawColumnName, ""));
      } else if (columnDataTypeMatcher.find()) {
        int columnNum = Integer.valueOf(entry.getKey().replaceAll("[^0-9]", ""));
        String rawColumnDataType = entry.getValue();
        if (columnDataTypeMap.containsKey(columnNum)) {
          String columnName = columnDataTypeMap.get(columnNum).getKey();
          columnDataTypeMap.put(columnNum, new Pair<String, String>(columnName, rawColumnDataType));
          continue;
        }
        columnDataTypeMap.put(columnNum, new Pair<String, String>("", rawColumnDataType));
      } else if (pkMatcher.find()) {
        String[] pkColumns = entry.getValue().split(",");
        for (String pk : pkColumns) {
          builder.addPrivateKeyColumn(pk.trim());
        }
      } else if (notNullMatcher.find()) {
        String[] notNullColumns = entry.getValue().split(",");
        for (String notNullColumn : notNullColumns) {
          builder.addNotNullColumn(notNullColumn.trim());
        }
      }
    }

    for (Map.Entry<Integer, Pair<String, String>> entry : columnDataTypeMap.entrySet()) {
      builder.addColumn(entry.getValue().getKey(), entry.getValue().getValue());
    }

    return builder.build();
  }

  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(String.format("[Schema] name: '%s'", name)).append("\n")
        .append("[Schema] Columns").append("\n");
    for (Map.Entry<String, Column> entry : columns.entrySet()) {
      builder.append(entry.getValue().toString()).append("\n");
    }
    return builder.toString();
  }

  public final String name;
  public final Map<String, Column> columns;
}
