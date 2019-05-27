import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Query {
  enum Type {
    SHOW("SHOW"), CREATE("CREATE"), DESCRIBE("DESCRIBE"), SELECT("SELECT"), INSERT("INSERT"),
    DELETE("DELETE"), UPDATE("UPDATE"), DROP_TABLE("DROP_TABLE"), INVALID("INVALID");

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
    Schema schema;
    String tableName;
    boolean isSelectAllColumns = false;
    List<String> selectedColumns = new ArrayList<String>();
    List<Condition> conditions = new ArrayList<Condition>();
    Map<String, Order> orders = new HashMap<String, Order>();
    List<String> ordersKeyOrder = new ArrayList<String>();
    Map<String, String> colValueMap = new HashMap<String, String>();
  }

  private Query(Builder builder) {
    this.type = builder.type;
    this.schema = builder.schema;
    this.tableName = builder.tableName;
    if (builder.isSelectAllColumns) {
      this.selectedColumns = "*";
    } else {
      StringBuilder strBuilder = new StringBuilder();
      for (String column : builder.selectedColumns) {
        strBuilder.append(column);
        if (!column.equals(builder.selectedColumns.get(builder.selectedColumns.size() - 1))) {
          strBuilder.append(", ");
        }
      }
      this.selectedColumns = strBuilder.toString();
    }
    this.conditions = new ArrayList<Condition>();
    for (Condition condition : builder.conditions) {
      this.conditions.add(condition.clone());
    }
    this.orders = new HashMap<String, Order>();
    for (Map.Entry<String, Order> entry : builder.orders.entrySet()) {
      this.orders.put(entry.getKey(), entry.getValue());
    }
    this.ordersKeyOrder = new ArrayList<String>();
    this.ordersKeyOrder.addAll(builder.ordersKeyOrder);
    this.colValueMap = new HashMap<String, String>();
    for (Map.Entry<String, String> entry : builder.colValueMap.entrySet()) {
      this.colValueMap.put(entry.getKey(), entry.getValue());
    }
  }

  public String toString() {
    StringBuilder strBuilder = new StringBuilder();
    strBuilder.append(type.getLabel());
    switch (type) {
      case SHOW: {
        strBuilder.append(" TABLES;");
        break;
      }
      case CREATE: {
        strBuilder.append(" TABLE ")
            .append(this.schema.name)
            .append(" (");
        for (String column : this.schema.columnOrder) {
          StringBuilder colBuilder = new StringBuilder();
          Schema.Column col = this.schema.columns.get(column);
          colBuilder.append(col.name)
              .append(" ")
              .append(col.dataType);
          if (!col.isNullable) {
            colBuilder.append(" NOT NULL");
          }
          if (col.isPrivateKey) {
            colBuilder.append(" PRIMARY KEY");
          }
          strBuilder.append(colBuilder);
          if (!column.equals(this.schema.columnOrder.get(this.schema.columnOrder.size() - 1))) {
            strBuilder.append(", ");
          }
        }
        strBuilder.append(");");
        break;
      }
      case DESCRIBE: {
        strBuilder.append(" ")
            .append(this.tableName)
            .append(";");
        break;
      }
      case SELECT: {
        strBuilder.append(" ")
            .append(this.selectedColumns)
            .append(" FROM ")
            .append(this.tableName);
        if (conditions.isEmpty()) {
          break;
        }
        strBuilder.append(" WHERE ");
        for (int i = 0; i < conditions.size(); ++i) {
          strBuilder.append(conditions.get(i).toString());
          if (i != conditions.size() - 1) {
            strBuilder.append(" ");
          }
        }
        if (orders.isEmpty()) {
          break;
        }
        strBuilder.append(" ORDER BY ");
        for (int i = 0; i < ordersKeyOrder.size(); ++i) {
          String col = ordersKeyOrder.get(i);
          Order order = orders.get(col);
          strBuilder.append(col)
              .append(" ")
              .append(order.getLabel());
          if (i != ordersKeyOrder.size() - 1) {
            strBuilder.append(", ");
          }
        }
        strBuilder.append(";");
        break;
      }
      case DELETE: {
        strBuilder.append(" FROM ")
            .append(this.tableName);
        if (this.conditions.isEmpty()) {
          break;
        }
        strBuilder.append(" WHERE ");
        for (int i = 0; i < conditions.size(); ++i) {
          strBuilder.append(conditions.get(i).toString());
          if (i != conditions.size() - 1) {
            strBuilder.append(" ");
          }
        }
        strBuilder.append(";");
        break;
      }
      default: {
        throw new RuntimeException("Invalid query type");
      }
    }
    return strBuilder.toString();
  }

  Type type;

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
