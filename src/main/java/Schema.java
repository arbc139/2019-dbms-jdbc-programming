
import javafx.util.Pair;
import util.FileParser;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Schema {
  public static class Column {
    public Column(String name, String dataType, boolean isPrimaryKey, boolean isNullable) {
      this.name = name;
      this.dataType = dataType;
      this.isPrimaryKey = isPrimaryKey;
      this.isNullable = isNullable;
    }

    public Column clone() {
      return new Column(name, dataType, isPrimaryKey, isNullable);
    }

    public String toString() {
      return String.format(
          "[Column] name: '%s', dataType: '%s', isPrimaryKey: '%b', isNullable: '%b'",
          name, dataType, isPrimaryKey, isNullable);
    }

    public boolean isNeedEscapedValue() {
      Pattern varcharPattern = Pattern.compile("^varchar\\([0-9]+\\)$");
      Pattern varchar2Pattern = Pattern.compile("^varchar2\\([0-9]+\\)$");
      Pattern charPattern = Pattern.compile("^char\\([0-9]+\\)$");
      Pattern datePattern = Pattern.compile("^date$");
      Pattern timePattern = Pattern.compile("^time$");

      String lowerDataType = dataType.toLowerCase();

      return varcharPattern.matcher(lowerDataType).find() ||
          varchar2Pattern.matcher(lowerDataType).find() ||
          charPattern.matcher(lowerDataType).find() ||
          datePattern.matcher(lowerDataType).find() ||
          timePattern.matcher(lowerDataType).find();
    }

    String name;
    String dataType;
    boolean isPrimaryKey;
    boolean isNullable;
  }

  public static class Builder {
    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    public Builder addColumn(String columnName, String columnDataType) {
      columnOrder.add(columnName);
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
        col.isPrimaryKey = true;
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
    List<String> columnOrder = new ArrayList<String>();
  }

  private Schema(Schema.Builder builder) {
    this.name = builder.name;
    this.columns = new HashMap<String, Column>();
    for (Map.Entry<String, Column> entry : builder.columns.entrySet()) {
      columns.put(entry.getKey(), entry.getValue().clone());
    }
    this.columnOrder = new ArrayList<String>();
    this.columnOrder.addAll(builder.columnOrder);
  }

  public static Schema parse(FileParser.KeyOrderMap tableDescription) {
    Schema.Builder builder = new Schema.Builder();

    Pattern namePattern = Pattern.compile("^Name$");
    Pattern columnPattern = Pattern.compile("^Column [0-9]+$");
    Pattern columnDataTypePattern = Pattern.compile("^Column [0-9]+ Data Type$");
    Pattern pkPattern = Pattern.compile("^PK$");
    Pattern notNullPattern = Pattern.compile("^Not NULL$");
    SortedMap<Integer, Pair<String, String>> columnDataTypeMap = new TreeMap<Integer, Pair<String, String>>();

    for (String key : tableDescription.keyOrder) {
      Matcher nameMatcher = namePattern.matcher(key);
      Matcher columnMatcher = columnPattern.matcher(key);
      Matcher columnDataTypeMatcher = columnDataTypePattern.matcher(key);
      Matcher pkMatcher = pkPattern.matcher(key);
      Matcher notNullMatcher = notNullPattern.matcher(key);

      if (nameMatcher.find()) {
        // Schema "Name"
        builder.setName(tableDescription.map.get(key));
      } else if (columnMatcher.find()) {
        int columnNum = Integer.valueOf(key.replaceAll("[^0-9]", ""));
        String rawColumnName = tableDescription.map.get(key);
        if (columnDataTypeMap.containsKey(columnNum)) {
          String dataType = columnDataTypeMap.get(columnNum).getValue();
          columnDataTypeMap.put(columnNum, new Pair<String, String>(rawColumnName, dataType));
          continue;
        }
        columnDataTypeMap.put(columnNum, new Pair<String, String>(rawColumnName, ""));
      } else if (columnDataTypeMatcher.find()) {
        int columnNum = Integer.valueOf(key.replaceAll("[^0-9]", ""));
        String rawColumnDataType = tableDescription.map.get(key);
        if (columnDataTypeMap.containsKey(columnNum)) {
          String columnName = columnDataTypeMap.get(columnNum).getKey();
          columnDataTypeMap.put(columnNum, new Pair<String, String>(columnName, rawColumnDataType));
          continue;
        }
        columnDataTypeMap.put(columnNum, new Pair<String, String>("", rawColumnDataType));
      } else if (pkMatcher.find()) {
        String[] pkColumns = tableDescription.map.get(key).split(",");
        for (String pk : pkColumns) {
          builder.addPrivateKeyColumn(pk.trim());
        }
      } else if (notNullMatcher.find()) {
        String[] notNullColumns = tableDescription.map.get(key).split(",");
        for (String notNullColumn : notNullColumns) {
          builder.addNotNullColumn(notNullColumn.trim());
        }
      }
    }

    for (int key : columnDataTypeMap.keySet()) {
      Pair<String, String> entry = columnDataTypeMap.get(key);
      builder.addColumn(entry.getKey(), entry.getValue());
    }

    return builder.build();
  }

  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(String.format("[Schema] name: '%s'", name)).append("\n")
        .append("[Schema] Columns").append("\n");
    for (String column : columnOrder) {
      builder.append(columns.get(column).toString()).append("\n");
    }
    return builder.toString();
  }

  public final String name;
  public final Map<String, Column> columns;
  public final List<String> columnOrder;
}
