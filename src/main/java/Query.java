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

    public Builder addCondition(String colName, Condition.Operator op, String value) {
      conditions.add(new Condition(colName, op, value));
      return this;
    }

    public Builder addOrder(String colName, String order) {
      orders.put(colName, order);
      return this;
    }

    public Builder addColValueSet(String colName, String value) {
      colValueMap.put(colName, value);
      return this;
    }

    public Query build() {
      return new Query(this);
    }

    Type type;
    Schema schema;
    String tableName;
    List<Condition> conditions = new ArrayList<Condition>();
    Map<String, String> orders = new HashMap<String, String>();
    Map<String, String> colValueMap = new HashMap<String, String>();
  }

  private Query(Builder builder) {
    this.type = builder.type;
    this.schema = builder.schema;
    this.tableName = builder.tableName;
    this.conditions = new ArrayList<Condition>();
    for (Condition condition : builder.conditions) {
      this.conditions.add(condition.clone());
    }
    this.orders = new HashMap<String, String>();
    for (Map.Entry<String, String> entry : builder.orders.entrySet()) {
      this.orders.put(entry.getKey(), entry.getValue());
    }
    this.colValueMap = new HashMap<String, String>();
    for (Map.Entry<String, String> entry : builder.colValueMap.entrySet()) {
      this.colValueMap.put(entry.getKey(), entry.getValue());
    }
  }

  Type type;

  // CREATE
  Schema schema;

  // DESCRIBE, SELECT, INSERT, DELETE, UPDATE, DROP_TABLE
  String tableName;

  // SELECT, DELETE, UPDATE
  List<Condition> conditions;
  Map<String, String> orders;

  // INSERT, UPDATE
  Map<String, String> colValueMap;
}
