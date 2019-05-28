import java.util.logging.Level;

public class Labeler {
  interface Label {
    public String get();
    public void print();
    public void println();
  }

  public enum ConsoleLabel implements Label {
    COMMON_TABLE_NAME_NOT_EXISTS,

    INSTRUCTION_INIT, COMMON_TRY_AGAIN,
    INSTRUCTION_IMPORT_CSV, INSTRUCTION_EXPORT_CSV, INSTRUCTION_MANIPULATE_DATA, INSTRUCTION_EXIT,

    IMPORT_CSV_TABLE_DESCRIPTION_SPECIFY_FILENAME,
    IMPORT_CSV_TABLE_DESCRIPTION_NEW_CREATE, IMPORT_CSV_TABLE_DESCRIPTION_ALREADY_EXISTS,
    IMPORT_CSV_INSERT_SPECIFY_CSV_FILE_NAME,
    IMPORT_CSV_INSERT_DUPLICATE_TUPLE,
    IMPORT_CSV_IMPORT_SUCCESS, IMPORT_CSV_IMPORT_FAILED,

    EXPORT_CSV_TABLE_NAME,
    EXPORT_CSV_CSV_FILE_NAME,
    EXPORT_CSV_EXPORT_SUCCESS, EXPORT_CSV_EXPORT_FAILURE,

    MANIPULATE_DATA_INIT,
    MANIPULATE_DATA_SHOW_TABLE_HEADER, MANIPULATE_DATA_SHOW_TABLE_FAILURE,
    MANIPULATE_DATA_DESCRIBE_SPECIFY_TABLE_NAME, MANIPULATE_DATA_DESCRIBE_HEADER,
    MANIPULATE_DATA_DESCRIBE_FAILURE,
    MANIPULATE_DATA_SELECT_SPECIFY_TABLE_NAME, MANIPULATE_DATA_SELECT_SPECIFY_COLUMNS,
    MANIPULATE_DATA_SELECT_ORDER_COLUMN, MANIPULATE_DATA_SELECT_ORDER_METHOD,
    MANIPULATE_DATA_SELECT_ORDER_COLUMN_METHOD_INVALID,
    MANIPULATE_DATA_SELECT_FAILURE,

    MANIPULATE_DATA_COMMON_COLUMN_NOT_EXIST,
    MANIPULATE_DATA_COMMON_SPECIFY_CONDITION_COLUMN, MANIPULATE_DATA_COMMON_SPECIFY_CONDITION_ARITHMETIC_OPERATOR,
    MANIPULATE_DATA_COMMON_SPECIFY_CONDITION_VALUE, MANIPULATE_DATA_COMMON_SPECIFY_CONDITION_LOGICAL_OPERATOR,
    MANIPULATE_DATA_COMMON_PRESS_ENTER;

    public String get() {
      switch (this) {
        // COMMON
        case COMMON_TABLE_NAME_NOT_EXISTS:
          return "Table not exists...";
        case COMMON_TRY_AGAIN:
          return "Invalid instruction code... try again...";
        // INSTRUCTION
        case INSTRUCTION_INIT:
          return "Please input the instruction number (1: Import from CSV, 2: Export to CSV, 3: Manipulate Data, 4: Exit): ";
        case INSTRUCTION_IMPORT_CSV:
          return "[Import from CSV]";
        case INSTRUCTION_EXPORT_CSV:
          return "[Export to CSV]";
        case INSTRUCTION_MANIPULATE_DATA:
          return "[Manipulate Data]";
        case INSTRUCTION_EXIT:
          return "[Exit program...]";
        // IMPORT_CSV
        case IMPORT_CSV_TABLE_DESCRIPTION_SPECIFY_FILENAME:
          return "Please specify the filename for table description : ";
        case IMPORT_CSV_TABLE_DESCRIPTION_NEW_CREATE:
          return "Table is newly created as described in the file.";
        case IMPORT_CSV_TABLE_DESCRIPTION_ALREADY_EXISTS:
          return "Table already exists.";
        case IMPORT_CSV_INSERT_SPECIFY_CSV_FILE_NAME:
          return "Please specify the CSV file name : ";
        case IMPORT_CSV_INSERT_DUPLICATE_TUPLE:
          return "Failed tuple : ";
        case IMPORT_CSV_IMPORT_SUCCESS:
          return "Data import completed.";
        case IMPORT_CSV_IMPORT_FAILED:
          return "Data import failure. (The number of columns does not match between the table description and the CSV file.)";
        // EXPORT_CSV
        case EXPORT_CSV_TABLE_NAME:
          return "Please specify the table name : ";
        case EXPORT_CSV_CSV_FILE_NAME:
          return "Please specify the CSV filename : ";
        case EXPORT_CSV_EXPORT_SUCCESS:
          return "Data export completed.";
        case EXPORT_CSV_EXPORT_FAILURE:
          return "Data export failure.";
        // MANIPULATE_DATA
        case MANIPULATE_DATA_INIT:
          return "Please input the instruction number (1: Show Tables, 2: Describe Table, 3: Select, 4: Insert, 5: Delete, 6: Update, 7: Drop Table, 8: Back to main) : ";
        case MANIPULATE_DATA_SHOW_TABLE_HEADER: {
          StringBuilder builder = new StringBuilder();
          return builder.append("=======\n")
              .append("Table List\n")
              .append("=======")
              .toString();
        }
        case MANIPULATE_DATA_SHOW_TABLE_FAILURE:
          return "SHOW TABLE failure.";
        case MANIPULATE_DATA_DESCRIBE_SPECIFY_TABLE_NAME:
          return "Please specify the table name : ";
        case MANIPULATE_DATA_DESCRIBE_HEADER: {
          StringBuilder builder = new StringBuilder();
          return builder.append("==========================================================\n")
              .append("Column Name | Data Type | Character Maximum Length(or Numeric Precision and Scale)\n")
              .append("==========================================================")
              .toString();
        }
        case MANIPULATE_DATA_DESCRIBE_FAILURE:
          return "DESCRIBE TABLE failure.";
        case MANIPULATE_DATA_SELECT_SPECIFY_TABLE_NAME:
          return "Please specify the table name : ";
        case MANIPULATE_DATA_SELECT_SPECIFY_COLUMNS:
          return "Please specify columns which you want to retrieve : ";
        case MANIPULATE_DATA_SELECT_ORDER_COLUMN:
          return "Please specify the column name for ordering (Press enter : skip) : ";
        case MANIPULATE_DATA_SELECT_ORDER_METHOD:
          return "Please specify the sorting criteria (Press enter : skip) : ";
        case MANIPULATE_DATA_SELECT_ORDER_COLUMN_METHOD_INVALID:
          return "There is an error on Order information... plz check it again...";
        case MANIPULATE_DATA_SELECT_FAILURE:
          return "<error detected>";
        case MANIPULATE_DATA_COMMON_COLUMN_NOT_EXIST:
          return "Target column not exists...";
        case MANIPULATE_DATA_COMMON_SPECIFY_CONDITION_COLUMN:
          return "Please specify the column which you want to make condition (Press enter : skip) : ";
        case MANIPULATE_DATA_COMMON_SPECIFY_CONDITION_ARITHMETIC_OPERATOR:
          return "Please specify the condition (1: =, 2: >, 3: < , 4: >=, 5: <=, 6: !=, 7: LIKE) : ";
        case MANIPULATE_DATA_COMMON_SPECIFY_CONDITION_VALUE:
          return "Please specify the condition value";
        case MANIPULATE_DATA_COMMON_SPECIFY_CONDITION_LOGICAL_OPERATOR:
          return "Please specify the condition (1: AND, 2: OR, 3: finish) : ";
        case MANIPULATE_DATA_COMMON_PRESS_ENTER:
          return "<Press enter>";
        // DEFAULT
        default:
          Main.LOG.log(Level.SEVERE, "Invalid console state");
          throw new RuntimeException("Invalid console state");
      }
    }

    public void print() {
      System.out.print(this.get());
    }

    public void println() {
      System.out.println(this.get());
    }
  }
}
