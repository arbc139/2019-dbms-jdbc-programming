import util.StringHelper;

import java.util.*;
import java.util.stream.Collectors;

public class Query {
  enum Type {
    CREATE("CREATE"), SELECT("SELECT"), INSERT("INSERT"), DELETE("DELETE"), UPDATE("UPDATE"),
    DROP_TABLE("DROP_TABLE"), INVALID("INVALID");

    private final String label;
    Type(String label) {
      this.label = label;
    }

    public String getLabel() {
      return label;
    }

    public static Type getType(String label) {
      for (Type type : Type.values()) {
        if (type.label.equals(label)) {
          return type;
        }
      }
      return INVALID;
    }
  }

  enum Order {
    ASC("ASC"), DESC("DESC"), INVALID("INVALID");

    private final String label;
    Order(String label) {
      this.label = label;
    }

    public String getLabel() {
      return label;
    }

    public static Order getOrder(String label) {
      for (Order order : Order.values()) {
        if (order.label.equals(label)) {
          return order;
        }
      }
      return INVALID;
    }
  }

  public static class Builder {
    public Builder setType(Type type) {
      this.type = type;
      return this;
    }

    public Builder setBaseSchemaName(String baseSchemaName) {
      this.baseSchemaName = baseSchemaName;
      return this;
    }

    public Builder setSchema(Schema schema) {
      this.schema = schema;
      return this;
    }

    public Builder setTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public Builder addSelectedColumn(String column) {
      if (column.equals("*")) {
        this.isSelectAllColumns = true;
        this.selectedColumns.clear();
        return this;
      }
      this.isSelectAllColumns = false;
      this.selectedColumns.add(column);
      return this;
    }

    public Builder addCondition(String colName, Condition.Operator op, String value) {
      conditions.add(new Condition(colName, op, value));
      return this;
    }

    public Builder addCondition(Condition.Operator andOrOp, String colName, Condition.Operator op, String value) {
      conditions.add(new Condition(andOrOp, colName, op, value));
      return this;
    }

    public Builder addOrder(String colName, Order order) {
      orders.put(colName, order);
      ordersKeyOrder.add(colName);
      return this;
    }

    public Builder addColValueSet(String colName, String value) {
      colValueMap.put(colName, value);
      return this;
    }

    public void clear() {
      this.type = Type.INVALID;
      this.schema = null;
      this.tableName = "";
      this.isSelectAllColumns = false;
      this.selectedColumns.clear();
      this.conditions.clear();
      this.orders.clear();
      this.ordersKeyOrder.clear();
      this.colValueMap.clear();
    }

    public Query build() {
      return new Query(this);
    }

    Type type;
    String baseSchemaName;
    Schema schema;
    String tableName;
    boolean isSelectAllColumns = false;
    List<String> selectedColumns = new ArrayList<>();
    List<Condition> conditions = new ArrayList<>();
    Map<String, Order> orders = new HashMap<>();
    List<String> ordersKeyOrder = new ArrayList<>();
    Map<String, String> colValueMap = new HashMap<>();
  }

  private Query(Builder builder) {
    this.type = builder.type;
    this.baseSchemaName = builder.baseSchemaName;
    this.schema = builder.schema;
    this.tableName = builder.tableName;
    if (builder.isSelectAllColumns) {
      this.selectedColumns = "*";
    } else {
      this.selectedColumns = String.join(
          ", ",
          builder.selectedColumns.stream()
              .map(StringHelper::escapeDoubleQuote)
              .collect(Collectors.toList()));
    }
    this.conditions = new ArrayList<>();
    for (Condition condition : builder.conditions) {
      this.conditions.add(condition.clone());
    }
    this.orders = new HashMap<>();
    for (Map.Entry<String, Order> entry : builder.orders.entrySet()) {
      this.orders.put(entry.getKey(), entry.getValue());
    }
    this.ordersKeyOrder = new ArrayList<>();
    this.ordersKeyOrder.addAll(builder.ordersKeyOrder);
    this.colValueMap = new HashMap<>();
    for (Map.Entry<String, String> entry : builder.colValueMap.entrySet()) {
      this.colValueMap.put(entry.getKey(), entry.getValue());
    }
  }

  public String toString() {
    StringBuilder strBuilder = new StringBuilder();
    strBuilder.append(type.getLabel());
    switch (type) {
      case CREATE: {
        strBuilder.append(" TABLE ")
            .append(StringHelper.escapeDoubleQuote(this.baseSchemaName))
            .append(".")
            .append(StringHelper.escapeDoubleQuote(this.schema.name))
            .append(" (");
        List<String> columnSchemas = this.schema.columnOrder.stream()
            .map(colName -> {
              Schema.Column col = this.schema.columns.get(colName);
              if (col.isNullable) {
                return String.format("%s %s", StringHelper.escapeDoubleQuote(col.name), col.dataType);
              } else {
                return String.format("%s %s NOT NULL", StringHelper.escapeDoubleQuote(col.name), col.dataType);
              }
            })
            .collect(Collectors.toList());
        strBuilder.append(String.join(", ", columnSchemas));
        List<String> primaryKeyCols = this.schema.columnOrder.stream()
            .filter(colName -> {
              Schema.Column col = this.schema.columns.get(colName);
              return col.isPrimaryKey;
            })
            .collect(Collectors.toList());
        if (!primaryKeyCols.isEmpty()) {
          strBuilder.append(", ");
          String primaryKeyLabel = StringHelper.escapeDoubleQuote(String.format("%s_PKEY", this.schema.name));
          strBuilder.append("CONSTRAINT ")
              .append(primaryKeyLabel)
              .append(" PRIMARY KEY (")
              .append(String.join(
                  ", ",
                  primaryKeyCols.stream()
                      .map(StringHelper::escapeDoubleQuote)
                      .collect(Collectors.toList())
              ))
              .append(")");
        }
        strBuilder.append(");");
        break;
      }
      case SELECT: {
        strBuilder.append(" ")
            .append(this.selectedColumns)
            .append(" FROM ")
            .append(StringHelper.escapeDoubleQuote(this.baseSchemaName))
            .append(".")
            .append(StringHelper.escapeDoubleQuote(this.tableName));
        if (!conditions.isEmpty()) {
          strBuilder.append(" WHERE ")
              .append(String.join(
                  " ",
                  conditions.stream()
                      .map(cond -> cond.toString(this.schema))
                      .collect(Collectors.toList())));
        }
        if (!orders.isEmpty()) {
          strBuilder.append(" ORDER BY ")
              .append(String.join(
                  ", ",
                  ordersKeyOrder.stream()
                      .map(col -> {
                        Order order = orders.get(col);
                        return String.format("%s %s", StringHelper.escapeDoubleQuote(col), order.getLabel());
                      })
                      .collect(Collectors.toList())));
        }
        strBuilder.append(";");
        break;
      }
      case DELETE: {
        strBuilder.append(" FROM ")
            .append(StringHelper.escapeDoubleQuote(this.baseSchemaName))
            .append(".")
            .append(StringHelper.escapeDoubleQuote(this.tableName));
        if (!this.conditions.isEmpty()) {
          strBuilder.append(" WHERE ")
              .append(String.join(
                  " ",
                  conditions.stream()
                      .map(cond -> cond.toString(this.schema))
                      .collect(Collectors.toList())));
        }
        strBuilder.append(";");
        break;
      }
      case INSERT: {
        strBuilder.append(" INTO ")
            .append(StringHelper.escapeDoubleQuote(this.baseSchemaName))
            .append(".")
            .append(StringHelper.escapeDoubleQuote(this.tableName))
            .append(" (");
        List<String> cols = new ArrayList<>(colValueMap.keySet());
        List<String> values = new ArrayList<>();
        for (String colName : cols) {
          String value = colValueMap.get(colName);
          Schema.Column col = this.schema.columns.get(colName);
          if (col.isNeedEscapedValue()) {
            values.add(StringHelper.escapeSingleQuote(value));
          } else {
            values.add(value);
          }
        }
        strBuilder.append(String.join(
                ", ",
                cols.stream()
                    .map(StringHelper::escapeDoubleQuote)
                    .collect(Collectors.toList())
            ))
            .append(") VALUES (")
            .append(String.join(", ", values))
            .append(")");
        if (!conditions.isEmpty()) {
          strBuilder.append(" WHERE ")
              .append(String.join(
                  " ",
                  conditions.stream()
                      .map(cond -> cond.toString(this.schema))
                      .collect(Collectors.toList())));
        }
        strBuilder.append(";");
        break;
      }
      case UPDATE: {
        strBuilder.append(" ")
            .append(StringHelper.escapeDoubleQuote(this.baseSchemaName))
            .append(".")
            .append(StringHelper.escapeDoubleQuote(this.tableName))
            .append(" SET ")
            .append(String.join(
                ", ",
                colValueMap.entrySet().stream()
                    .map(entry -> {
                      Schema.Column col = this.schema.columns.get(entry.getKey());
                      String value;
                      if (col.isNeedEscapedValue()) {
                        value = StringHelper.escapeSingleQuote(entry.getValue());
                      } else {
                        value = entry.getValue();
                      }
                      return String.format("%s = %s", StringHelper.escapeDoubleQuote(entry.getKey()), value);
                    })
                    .collect(Collectors.toList())))
            .append(" WHERE ")
            .append(String.join(
                " ",
                conditions.stream()
                    .map(cond -> cond.toString(this.schema))
                    .collect(Collectors.toList())))
            .append(";");
        break;
      }
      default: {
        throw new RuntimeException("Invalid query type");
      }
    }
    return strBuilder.toString();
  }

  Type type;

  String baseSchemaName;

  // CREATE
  Schema schema;

  // DESCRIBE, SELECT, INSERT, DELETE, UPDATE, DROP_TABLE
  String tableName;

  // SELECT
  String selectedColumns;

  // SELECT, DELETE, UPDATE
  List<Condition> conditions;
  Map<String, Order> orders;
  List<String> ordersKeyOrder;

  // INSERT, UPDATE
  Map<String, String> colValueMap;
}
