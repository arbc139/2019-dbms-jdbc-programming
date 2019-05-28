
import javafx.util.Pair;
import util.TxtCsvParser;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Schema {
  public static class Column {
    enum JavaType {
      STRING, INTEGER, DOUBLE,
    }

    public static String convertDateType(String rawColumnType, int size, int precision, int scale) {
      Pattern intPattern = Pattern.compile("^int[0-9]*$");
      Pattern varcharPattern = Pattern.compile("^varchar(1|2)?$");
      Pattern numericPattern = Pattern.compile("^numeric$");
      Pattern charPattern = Pattern.compile("^bpchar$");
      Pattern datePattern = Pattern.compile("^date$");
      Pattern timePattern = Pattern.compile("^time$");
      Pattern namePattern = Pattern.compile("^name$");
      Pattern boolPattern = Pattern.compile("^bool$");

      if (intPattern.matcher(rawColumnType).find()) {
        return "INTEGER";
      } else if (varcharPattern.matcher(rawColumnType).find()) {
        return String.format("VARCHAR(%d)", size);
      } else if (numericPattern.matcher(rawColumnType).find()) {
        return String.format("NUMERIC(%d, %d)", precision, scale);
      } else if (charPattern.matcher(rawColumnType).find()) {
        return String.format("CHAR(%d)", size);
      } else if (datePattern.matcher(rawColumnType).find()) {
        return "DATE";
      } else if (timePattern.matcher(rawColumnType).find()) {
        return "TIME";
      } else if (namePattern.matcher(rawColumnType).find()) {
        return "NAME";
      } else if (boolPattern.matcher(rawColumnType).find()) {
        return "BOOL";
      } else {
        return "NONE";
      }
    }

    public JavaType getDataTypeToJavaType() {
      Matcher integerMatcher = Pattern.compile("^integer$").matcher(dataType);
      Matcher numericMatcher = Pattern.compile("^numeric\\([0-9]+, [0-9]+\\)$").matcher(dataType);

      if (integerMatcher.find()) {
        return JavaType.INTEGER;
      } else if (numericMatcher.find()) {
        return JavaType.DOUBLE;
      } else {
        return JavaType.STRING;
      }
    }

    public String getDataTypeWithoutSize() {
      return dataType.replaceAll("\\(.*\\)", "");
    }

    public Column(String name, String dataType, boolean isPrimaryKey, boolean isNullable) {
      this.name = name;
      this.dataType = dataType;
      this.isPrimaryKey = isPrimaryKey;
      this.isNullable = isNullable;
    }

    public Column(String name, String dataType, boolean isPrimaryKey, boolean isNullable,
                  int size, int precision, int scale) {
      this.name = name;
      this.dataType = dataType;
      this.isPrimaryKey = isPrimaryKey;
      this.isNullable = isNullable;
      this.size = size;
      this.precision = precision;
      this.scale = scale;
    }

    public Column clone() {
      return new Column(name, dataType, isPrimaryKey, isNullable, size, precision, scale);
    }

    public String toString() {
      return String.format(
          "[Column] name: '%s', dataType: '%s', isPrimaryKey: '%b', isNullable: '%b', size: '%d', precision: '%d', scale: '%d'",
          name, dataType, isPrimaryKey, isNullable, size, precision, scale);
    }

    public boolean isNeedEscapedValue() {
      Pattern varcharPattern = Pattern.compile("^varchar\\([0-9]+\\)$");
      Pattern varchar2Pattern = Pattern.compile("^varchar2\\([0-9]+\\)$");
      Pattern charPattern = Pattern.compile("^char\\([0-9]+\\)$");
      Pattern datePattern = Pattern.compile("^date$");
      Pattern timePattern = Pattern.compile("^time$");
      Pattern namePattern = Pattern.compile("^name$");

      String lowerDataType = dataType.toLowerCase();

      return varcharPattern.matcher(lowerDataType).find() ||
          varchar2Pattern.matcher(lowerDataType).find() ||
          charPattern.matcher(lowerDataType).find() ||
          datePattern.matcher(lowerDataType).find() ||
          timePattern.matcher(lowerDataType).find() ||
          namePattern.matcher(lowerDataType).find();
    }

    String name;
    String dataType;
    boolean isPrimaryKey;
    boolean isNullable;

    // Only use in DESCRIBE...
    int size = -1;
    int precision = -1;
    int scale = -1;
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

    public Builder addColumnWithSizeInfo(String columnName, String columnDataType, int size,
                                         int precision, int scale) {
      columnOrder.add(columnName);
      if (columns.containsKey(columnName)) {
        Column col = columns.get(columnName);
        col.name = columnName;
        col.dataType = columnDataType;
        col.size = size;
        col.precision = precision;
        col.scale = scale;
        return this;
      }
      columns.put(
          columnName,
          new Column(columnName, columnDataType, false, true,
              size, precision, scale));
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

  public static Schema parse(TxtCsvParser.KeyOrderMap tableDescription) {
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

  public static Schema getSchema(String baseSchemaName, String tableName, Statement st) throws SQLException {
    Query query = new Query.Builder()
        .setType(Query.Type.SELECT)
        .setBaseSchemaName(baseSchemaName)
        .setTableName(tableName)
        .addSelectedColumn("*")
        .build();
    try {
      ResultSet rs = st.executeQuery(query.toString());
      return Schema.parse(tableName, rs.getMetaData());
    } catch (SQLException e) {
      if (e.getSQLState().equals("42P01")) {
        // Relation not exists error
        return null;
      } else {
        throw e;
      }
    }
  }

  public static Schema parse(String tableName, ResultSetMetaData meta) throws SQLException {
    Schema.Builder builder = new Schema.Builder();

    builder.setName(tableName);
    for (int i = 1; i <= meta.getColumnCount(); ++i) {
      String columnName = meta.getColumnName(i);
      String columnType = Column.convertDateType(
          meta.getColumnTypeName(i),
          meta.getColumnDisplaySize(i),
          meta.getPrecision(i),
          meta.getScale(i));
      builder.addColumnWithSizeInfo(
          columnName, columnType, meta.getColumnDisplaySize(i), meta.getPrecision(i), meta.getScale(i));
      if (meta.isNullable(i) == 0) {
        builder.addNotNullColumn(columnName);
      }
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

  public List<String> getDescribes() {
    List<String> rows = new ArrayList<>();
    for (String colName : columnOrder) {
      List<String> colInfos = new ArrayList<>();
      Column col = columns.get(colName);
      colInfos.add(col.name);
      colInfos.add(col.getDataTypeWithoutSize());
      if (col.isNeedEscapedValue()) {
        colInfos.add(String.valueOf(col.size));
      } else {
        colInfos.add(String.format("(%d,%d)", col.precision, col.scale));
      }
      rows.add(String.join(", ", colInfos));
    }
    return rows;
  }

  public final String name;
  public final Map<String, Column> columns;
  public final List<String> columnOrder;
}
