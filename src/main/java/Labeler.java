import java.util.logging.Level;

public class Labeler {
  interface Label {
    public String get();
    public void print();
    public void println();
  }

  public enum ConsoleLabel implements Label {
    INSTRUCTION_INIT, INSTRUCTION_TRY_AGAIN,
    INSTRUCTION_IMPORT_CSV, INSTRUCTION_EXPORT_CSV, INSTRUCTION_MANIPULATE_DATA, INSTRUCTION_EXIT,

    IMPORT_CSV_TABLE_DESCRIPTION_SPECIFY_FILENAME,
    IMPORT_CSV_TABLE_DESCRIPTION_NEW_CREATE, IMPORT_CSV_TABLE_DESCRIPTION_ALREADY_EXISTS,
    IMPORT_CSV_INSERT_SPECIFY_CSV_FILE_NAME,
    IMPORT_CSV_INSERT_DUPLICATE_TUPLE,
    IMPORT_CSV_IMPORT_SUCCESS, IMPORT_CSV_IMPORT_FAILED,

    EXPORT_CSV_TABLE_NAME,
    EXPORT_CSV_TABLE_NAME_NOT_EXISTS,
    EXPORT_CSV_CSV_FILE_NAME,
    EXPORT_CSV_EXPORT_SUCCESS, EXPORT_CSV_EXPORT_FAILURE;

    public String get() {
      switch (this) {
        // INSTRUCTION
        case INSTRUCTION_INIT:
          return "Please input the instruction number (1: Import from CSV, 2: Export to CSV, 3: Manipulate Data, 4: Exit): ";
        case INSTRUCTION_TRY_AGAIN:
          return "Invalid instruction code... try again...";
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
        case EXPORT_CSV_TABLE_NAME_NOT_EXISTS:
          return "Table not exists...";
        case EXPORT_CSV_CSV_FILE_NAME:
          return "Please specify the CSV filename : ";
        case EXPORT_CSV_EXPORT_SUCCESS:
          return "Data export completed.";
        case EXPORT_CSV_EXPORT_FAILURE:
          return "Data export failure.";
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
