import util.StringHelper;

public class Condition {
  enum Operator {
    INVALID("INVALID"),
    AND("AND"), OR("OR"),
    EQ("="), GT(">"), LT("<"), GTE(">="), LTE("<="), NEQ("!="), LIKE("LIKE");

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
    this.andOrOp = Operator.INVALID;
    this.columnName = columnName;
    this.op = op;
    this.value = value;
  }

  public Condition(Operator andOrOp, String columnName, Operator op, String value) {
    this.andOrOp = andOrOp;
    this.columnName = columnName;
    this.op = op;
    this.value = value;
  }

  public String toString() {
    StringBuilder strBuilder = new StringBuilder();
    if (andOrOp != Operator.INVALID) {
      strBuilder.append(andOrOp.getLabel())
          .append(" ");
    }
    strBuilder.append(StringHelper.escape(columnName))
        .append(" ")
        .append(op.getLabel())
        .append(" ");
    if (op == Operator.LIKE) {
      strBuilder.append(StringHelper.escape(value));
    } else {
      strBuilder.append(value);
    }
    return strBuilder.toString();
  }

  public String toString(Schema schema) {
    StringBuilder strBuilder = new StringBuilder();
    if (andOrOp != Operator.INVALID) {
      strBuilder.append(andOrOp.getLabel())
          .append(" ");
    }
    strBuilder.append(StringHelper.escape(columnName))
        .append(" ")
        .append(op.getLabel())
        .append(" ");
    Schema.Column column = schema.columns.get(columnName);
    if (column.isNeedEscapedValue()) {
      strBuilder.append(StringHelper.escape(value));
    } else {
      strBuilder.append(value);
    }
    return strBuilder.toString();
  }

  public Condition clone() {
    return new Condition(this.andOrOp, this.columnName, this.op, this.value);
  }

  public Operator andOrOp;
  public String columnName;
  public Operator op;
  public String value;
}
