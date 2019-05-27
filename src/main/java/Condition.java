public class Condition {
  enum Operator {
    INVALID("INVALID"), EQ("="), GT(">"), LT("<"), GTE(">="), LTE("<="), NEQ("!="), LIKE("LIKE");

    private final String label;
    Operator(String label) {
      this.label = label;
    }

    public String getLabel() {
      return label;
    }

    public static Operator getOperator(String label) {
      for (Operator op : Operator.values()) {
        if (op.label.equals(label)) {
          return op;
        }
      }
      return INVALID;
    }
  }

  public Condition(String columnName, Operator op, String value) {
    this.columnName = columnName;
    this.op = op;
    this.value = value;
  }

  public Condition clone() {
    return new Condition(this.columnName, this.op, this.value);
  }

  public String columnName;
  public Operator op;
  public String value;
}
