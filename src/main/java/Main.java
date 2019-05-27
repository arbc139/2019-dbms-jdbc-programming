import util.FileParser;
import util.Printer;

import java.io.FileNotFoundException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
  private static final String DB_DRIVER = "org.postgresql.Driver";
  private static final String DB_CONNECTION_FILE = "connection.txt";
  public static final Logger LOG = Logger.getLogger("PROJECT 3");

  public static void main(String args[]) throws ClassNotFoundException {
    Class.forName(DB_DRIVER);

    // Create PostgreSQL Connection
    PsqlConnection conn;
    try {
      FileParser txtParser = new FileParser();
      try {
        txtParser.open(DB_CONNECTION_FILE);
      } catch (FileNotFoundException e) {
        Main.LOG.log(Level.SEVERE, String.format("%s is not exist", DB_CONNECTION_FILE));
        throw new RuntimeException(e);
      }
      Map<String, String> connectionInfo = txtParser.parseTxt();
      txtParser.close();
      Printer.printMap(connectionInfo);

      String connectionUrl = String.format(
          "jdbc:postgresql://%s/%s", connectionInfo.get(PsqlConnection.PSQL_CONNECTION_IP_KEY),
          connectionInfo.get(PsqlConnection.PSQL_CONNECTION_DB_NAME_KEY));

      Properties props = new Properties();
      props.setProperty("user", connectionInfo.get(PsqlConnection.PSQL_CONNECTION_ID_KEY));
      props.setProperty("password", connectionInfo.get(PsqlConnection.PSQL_CONNECTION_PW_KEY));
      props.setProperty("currentSchema", connectionInfo.get(PsqlConnection.PSQL_CONNECTION_SCHEMA_NAME_KEY));

      conn = PsqlConnection.create(DriverManager.getConnection(connectionUrl, props), connectionInfo);
    } catch (SQLException e) {
      LOG.log(Level.SEVERE, "Connection Failed");
      throw new RuntimeException(e);
    }

    while (true) {
      Scanner input = new Scanner(System.in);
      int code;

      Labeler.ConsoleLabel.INSTRUCTION_INIT.print();
      try {
        code = input.nextInt();
      } catch (InputMismatchException e) {
        Labeler.ConsoleLabel.INSTRUCTION_TRY_AGAIN.println();
        continue;
      }
      Instruction inst = Instruction.getInstruction(code);
      switch (inst) {
        case IMPORT_CSV: {
          Labeler.ConsoleLabel.INSTRUCTION_IMPORT_CSV.println();
          importCsv(conn);
          break;
        }
        case EXPORT_CSV: {
          Labeler.ConsoleLabel.INSTRUCTION_EXPORT_CSV.println();
          exportCsv(conn);
          break;
        }
        case MANIPULATE_DATA: {
          Labeler.ConsoleLabel.INSTRUCTION_MANIPULATE_DATA.println();
          manipulateData(conn);
          break;
        }
        case EXIT: {
          Labeler.ConsoleLabel.INSTRUCTION_EXIT.println();
          return;
        }
        case TEST_BUILD_QUERY: {
          testQueryBuild(conn);
          break;
        }
        case INVALID: {
          Labeler.ConsoleLabel.INSTRUCTION_TRY_AGAIN.println();
          continue;
        }
      }
      System.out.println();
    }
  }

  private static void importCsv(PsqlConnection conn) {
    Scanner input = new Scanner(System.in);
    Labeler.ConsoleLabel.IMPORT_CSV_TABLE_DESCRIPTION_SPECIFY_FILENAME.print();
    String tableDescriptionFileName = input.nextLine();

    FileParser txtParser = new FileParser();
    try {
      txtParser.open(tableDescriptionFileName);
    } catch (FileNotFoundException e) {
      LOG.log(Level.SEVERE, "Table Description File does not exist.");
      throw new RuntimeException(e);
    }
    FileParser.KeyOrderMap tableDescription = txtParser.parseOrderedTxt();
    txtParser.close();

    // TODO(totoro): Create Table
    Schema schema = Schema.parse(tableDescription);
    System.out.println(schema);
    Query.Builder queryBuilder = new Query.Builder();
    queryBuilder.setType(Query.Type.CREATE)
        .setBaseSchemaName(conn.getBaseSchemaName())
        .setSchema(schema);
    Query query = queryBuilder.build();
    System.out.println(query);

    boolean isTableAlreadyExists = false;
    if (isTableAlreadyExists) {
      Labeler.ConsoleLabel.IMPORT_CSV_TABLE_DESCRIPTION_ALREADY_EXISTS.println();
    } else {
      Labeler.ConsoleLabel.IMPORT_CSV_TABLE_DESCRIPTION_NEW_CREATE.println();
    }

    Labeler.ConsoleLabel.IMPORT_CSV_INSERT_SPECIFY_CSV_FILE_NAME.print();
    String csvFileName = input.nextLine();

    FileParser csvParser = new FileParser();
    try {
      csvParser.open(csvFileName);
    } catch (FileNotFoundException e) {
      LOG.log(Level.SEVERE, "CSV File does not exist.");
      throw new RuntimeException(e);
    }
    List<Map<String, String>> csvRows = csvParser.parseCsv();
    for (Map<String, String> row : csvRows) {
      Printer.printMap(row);
    }
    csvParser.close();

    // TODO(totoro): Insert rows from CSV
    // QueryGenerator.insert(tableDescription, csvRows);

    int insertionSuccessCount = 0;
    int insertionFailureCount = 0;
    System.out.println(String.format(
        "%s (Insertion Success : %d, Insertion Failure : %d)",
        Labeler.ConsoleLabel.IMPORT_CSV_IMPORT_SUCCESS.get(), insertionSuccessCount, insertionFailureCount));
  }

  private static void exportCsv(PsqlConnection conn) {
    Scanner input = new Scanner(System.in);
    Labeler.ConsoleLabel.EXPORT_CSV_TABLE_NAME.print();
    String tableName = input.nextLine();

    // TODO(totoro): Get table rows
    // QueryGenerator.selectAll(tableName);

    boolean isTableExists = true;
    if (!isTableExists) {
      Labeler.ConsoleLabel.EXPORT_CSV_TABLE_NAME_NOT_EXISTS.println();
      Labeler.ConsoleLabel.EXPORT_CSV_EXPORT_FAILURE.println();
      return;
    }

    Labeler.ConsoleLabel.EXPORT_CSV_CSV_FILE_NAME.print();
    String csvFileName = input.nextLine();

    // TODO(totoro): Export rows to CSV

    Labeler.ConsoleLabel.EXPORT_CSV_EXPORT_SUCCESS.println();
  }

  private static void manipulateData(PsqlConnection conn) {
    // TODO(totoro): Implements manipulateData logics...
  }

  private static void testQueryBuild(PsqlConnection conn) {
    String schemaName = conn.getBaseSchemaName();
    Query.Builder queryBuilder = new Query.Builder();
    Schema.Builder schemaBuilder = new Schema.Builder();
    schemaBuilder.setName("TEST_TABLE")
        .addColumn("col1", "INTEGER")
        .addColumn("col2", "NUMERIC(2, 1)")
        .addColumn("col3", "VARCHAR(100)")
        .addColumn("col4", "VARCHAR2(100)")
        .addColumn("col5", "DATE")
        .addColumn("col6", "TIME")
        .addNotNullColumn("col1")
        .addNotNullColumn("col2")
        .addNotNullColumn("col3")
        .addPrivateKeyColumn("col1")
        .addPrivateKeyColumn("col2");
    Schema schema = schemaBuilder.build();

    {
      // Test SHOW TABLES
      queryBuilder.setType(Query.Type.SHOW)
          .setBaseSchemaName(schemaName);
      System.out.println(queryBuilder.build());
      queryBuilder.clear();
    }
    {
      // Test CREATE
      queryBuilder.setType(Query.Type.CREATE)
          .setBaseSchemaName(schemaName)
          .setSchema(schema);
      System.out.println(queryBuilder.build());
      queryBuilder.clear();
    }
    {
      // Test DESCRIBE
      queryBuilder.setType(Query.Type.DESCRIBE)
          .setBaseSchemaName(schemaName)
          .setTableName("TEST_TABLE")
          .setSchema(schema);
      System.out.println(queryBuilder.build());
      queryBuilder.clear();
    }
    {
      // Test SELECT
      // SELECT * FROM TEST_TABLE;
      queryBuilder.setType(Query.Type.SELECT)
          .setBaseSchemaName(schemaName)
          .setTableName("TEST_TABLE")
          .setSchema(schema)
          .addSelectedColumn("*");
      System.out.println(queryBuilder.build());
      queryBuilder.clear();

      // SELECT col1, col2 FROM TEST_TABLE;
      queryBuilder.setType(Query.Type.SELECT)
          .setBaseSchemaName(schemaName)
          .setTableName("TEST_TABLE")
          .setSchema(schema)
          .addSelectedColumn("col1")
          .addSelectedColumn("col2");
      System.out.println(queryBuilder.build());
      queryBuilder.clear();

      // SELECT col1, col2 FROM TEST_TABLE WHERE col1 > 10;
      queryBuilder.setType(Query.Type.SELECT)
          .setBaseSchemaName(schemaName)
          .setTableName("TEST_TABLE")
          .setSchema(schema)
          .addSelectedColumn("col1")
          .addSelectedColumn("col2")
          .addCondition("col1", Condition.Operator.GT, "10");
      System.out.println(queryBuilder.build());
      queryBuilder.clear();

      // SELECT col1, col2 FROM TEST_TABLE WHERE col1 > 10 OR col2 < 30.2;
      queryBuilder.setType(Query.Type.SELECT)
          .setBaseSchemaName(schemaName)
          .setTableName("TEST_TABLE")
          .setSchema(schema)
          .addSelectedColumn("col1")
          .addSelectedColumn("col2")
          .addCondition("col1", Condition.Operator.GT, "10")
          .addCondition(Condition.Operator.OR, "col2", Condition.Operator.LT, "30.2");
      System.out.println(queryBuilder.build());
      queryBuilder.clear();

      // SELECT col1, col2, col3 FROM TEST_TABLE WHERE col1 > 10 OR col2 < 30.2 AND col3 LIKE '%me%';
      queryBuilder.setType(Query.Type.SELECT)
          .setBaseSchemaName(schemaName)
          .setTableName("TEST_TABLE")
          .setSchema(schema)
          .addSelectedColumn("col1")
          .addSelectedColumn("col2")
          .addSelectedColumn("col3")
          .addCondition("col1", Condition.Operator.GT, "10")
          .addCondition(Condition.Operator.OR, "col2", Condition.Operator.LT, "30.2")
          .addCondition(Condition.Operator.AND, "col3", Condition.Operator.LIKE, "%me%");
      System.out.println(queryBuilder.build());
      queryBuilder.clear();

      // SELECT col1, col2, col3 FROM TEST_TABLE WHERE col1 > 10 OR col2 < 30.2 AND col3 LIKE '%me%' AND col4 LIKE '%too%' AND col5 = "2019-01-01" AND col6 = "08:00:00";
      queryBuilder.setType(Query.Type.SELECT)
          .setBaseSchemaName(schemaName)
          .setTableName("TEST_TABLE")
          .setSchema(schema)
          .addSelectedColumn("col1")
          .addSelectedColumn("col2")
          .addSelectedColumn("col3")
          .addCondition("col1", Condition.Operator.GT, "10")
          .addCondition(Condition.Operator.OR, "col2", Condition.Operator.LT, "30.2")
          .addCondition(Condition.Operator.AND, "col3", Condition.Operator.LIKE, "%me%")
          .addCondition(Condition.Operator.AND, "col4", Condition.Operator.LIKE, "%too%")
          .addCondition(Condition.Operator.AND, "col5", Condition.Operator.EQ, "2019-01-01")
          .addCondition(Condition.Operator.AND, "col6", Condition.Operator.EQ, "08:00:00");
      System.out.println(queryBuilder.build());
      queryBuilder.clear();

      // SELECT col1, col2, col3 FROM TEST_TABLE WHERE col1 > 10 OR col2 < 30.2 AND col3 LIKE '%me%' ORDER BY col1 ASC;
      queryBuilder.setType(Query.Type.SELECT)
          .setBaseSchemaName(schemaName)
          .setTableName("TEST_TABLE")
          .setSchema(schema)
          .addSelectedColumn("col1")
          .addSelectedColumn("col2")
          .addSelectedColumn("col3")
          .addCondition("col1", Condition.Operator.GT, "10")
          .addCondition(Condition.Operator.OR, "col2", Condition.Operator.LT, "30.2")
          .addCondition(Condition.Operator.AND, "col3", Condition.Operator.LIKE, "%me%")
          .addOrder("col1", Query.Order.ASC);
      System.out.println(queryBuilder.build());
      queryBuilder.clear();

      // SELECT col1, col2, col3 FROM TEST_TABLE WHERE col1 > 10 OR col2 < 30.2 AND col3 LIKE '%me%' ORDER BY col1 ASC, col2 DESC;
      queryBuilder.setType(Query.Type.SELECT)
          .setBaseSchemaName(schemaName)
          .setTableName("TEST_TABLE")
          .setSchema(schema)
          .addSelectedColumn("col1")
          .addSelectedColumn("col2")
          .addSelectedColumn("col3")
          .addCondition("col1", Condition.Operator.GT, "10")
          .addCondition(Condition.Operator.OR, "col2", Condition.Operator.LT, "30.2")
          .addCondition(Condition.Operator.AND, "col3", Condition.Operator.LIKE, "%me%")
          .addOrder("col1", Query.Order.ASC)
          .addOrder("col2", Query.Order.DESC);
      System.out.println(queryBuilder.build());
      queryBuilder.clear();
    }
    {
      // Test DELETE
      // DELETE FROM TEST_TABLE WHERE col1 > 10;
      queryBuilder.setType(Query.Type.DELETE)
          .setBaseSchemaName(schemaName)
          .setTableName("TEST_TABLE")
          .setSchema(schema)
          .addCondition("col1", Condition.Operator.GT, "10");
      System.out.println(queryBuilder.build());
      queryBuilder.clear();

      // DELETE FROM TEST_TABLE WHERE col1 > 10 OR col2 < 30.2;
      queryBuilder.setType(Query.Type.DELETE)
          .setBaseSchemaName(schemaName)
          .setTableName("TEST_TABLE")
          .setSchema(schema)
          .addCondition("col1", Condition.Operator.GT, "10")
          .addCondition(Condition.Operator.OR, "col2", Condition.Operator.LT, "30.2");
      System.out.println(queryBuilder.build());
      queryBuilder.clear();

      // DELETE FROM TEST_TABLE WHERE col1 > 10 OR col2 < 30.2 AND col3 LIKE '%me%';
      queryBuilder.setType(Query.Type.DELETE)
          .setBaseSchemaName(schemaName)
          .setTableName("TEST_TABLE")
          .setSchema(schema)
          .addCondition("col1", Condition.Operator.GT, "10")
          .addCondition(Condition.Operator.OR, "col2", Condition.Operator.LT, "30.2")
          .addCondition(Condition.Operator.AND, "col3", Condition.Operator.LIKE, "%me%");
      System.out.println(queryBuilder.build());
      queryBuilder.clear();
    }
    {
      // Test INSERT
      // INSERT INTO TEST_TABLE (col1, col2, col3, col4, col5, col6) VALUES (1, 2.2, "abc", "def", "2019-01-01", "08:00:00") WHERE col1 > 10;
      queryBuilder.setType(Query.Type.INSERT)
          .setBaseSchemaName(schemaName)
          .setTableName("TEST_TABLE")
          .setSchema(schema)
          .addColValueSet("col1", "1")
          .addColValueSet("col2", "2.2")
          .addColValueSet("col3", "abc")
          .addColValueSet("col4", "def")
          .addColValueSet("col5", "2019-01-01")
          .addColValueSet("col6", "08:00:00")
          .addCondition("col1", Condition.Operator.GT, "10");
      System.out.println(queryBuilder.build());
      queryBuilder.clear();
    }
    {
      // Test UPDATE
      // UPDATE TEST_TABLE SET col1 = 1, col2 = 2.2, col3 = "abc", col4 = "def", col5 = "2019-01-01", col6 = "08:00:00" WHERE col1 > 10;
      queryBuilder.setType(Query.Type.UPDATE)
          .setBaseSchemaName(schemaName)
          .setTableName("TEST_TABLE")
          .setSchema(schema)
          .addColValueSet("col1", "1")
          .addColValueSet("col2", "2.2")
          .addColValueSet("col3", "abc")
          .addColValueSet("col4", "def")
          .addColValueSet("col5", "2019-01-01")
          .addColValueSet("col6", "08:00:00")
          .addCondition("col1", Condition.Operator.GT, "10");
      System.out.println(queryBuilder.build());
      queryBuilder.clear();
    }
  }
}

//        Properties props = new Properties();
//
//        /* Setting Connection Info */
//        props.setProperty("user", 		DB_USER);
//        props.setProperty("password", 	DB_PASSWORD);
//
//        /* Connect! */
//        Connection conn = DriverManager.getConnection(DB_CONNECTION_URL, props);
//
//        Statement st = conn.createStatement();
//
//        /* Create Table SQL */
//        String CreateTableSQL = "CREATE TABLE student_table " +
//                "(ID int, " +
//                "name varchar(20) not null, " +
//                "address varchar(50) not null," +
//                "department_ID int," +
//                "primary key (ID))";
//
//        st.executeUpdate(CreateTableSQL);
//
//        /* Insert Row using Statement */
//        String InsertSQL_1 = "INSERT INTO student_table values(1, 'Brandt', 'addr1', 1)";
//        st.executeUpdate(InsertSQL_1);
//
//        /* Insert Row using PreparedStatement */
//        String InsertSQL_2 = "INSERT INTO student_table (ID, name, address, department_ID) values(?, ?, ?, ?)";
//
//        PreparedStatement preparedStmt = conn.prepareStatement(InsertSQL_2);
//        preparedStmt.setInt(1, 2);
//        preparedStmt.setString(2, "Chavez");
//        preparedStmt.setString(3, "addr2");
//        preparedStmt.setInt(4, 2);
//
//        preparedStmt.execute();
//        ResultSet rs = st.executeQuery("SELECT ID, name, address, department_ID FROM student_table");
//
//        System.out.println("============ RESULT ============");
//        while (rs.next()) {
//            System.out.print("ID : " + rs.getString(1) + ", ");
//            System.out.print("Name : " + rs.getString(2) + ", ");
//            System.out.print("Address : " + rs.getString(3) + ", ");
//            System.out.print("Department_ID : " + rs.getString(4));
//            System.out.println();
//        }
//
//        /* Update Row */
//        String UpdateSQL = "UPDATE student_table SET address = ? where ID = ?";
//
//        preparedStmt = conn.prepareStatement(UpdateSQL);
//        preparedStmt.setString(1, "addr3");
//        preparedStmt.setInt(2, 2);
//        preparedStmt.executeUpdate();
//
//        rs = st.executeQuery("SELECT ID, name, address, department_ID FROM student_table");
//
//        System.out.println("============ RESULT ============");
//        while (rs.next()) {
//            System.out.print("ID : " + rs.getString(1) + ", ");
//            System.out.print("Name : " + rs.getString(2) + ", ");
//            System.out.print("Address : " + rs.getString(3) + ", ");
//            System.out.print("Department_ID : " + rs.getString(4));
//            System.out.println();
//        }
//        /* Delete Row */
//        String DeleteSQL = "DELETE FROM student_table where ID = 2";
//        st.executeUpdate(DeleteSQL);
//
//        rs = st.executeQuery("SELECT ID, name, address, department_ID FROM student_table");
//
//        System.out.println("============ RESULT ============");
//        while (rs.next()) {
//            System.out.print("ID : " + rs.getString(1) + ", ");
//            System.out.print("Name : " + rs.getString(2) + ", ");
//            System.out.print("Address : " + rs.getString(3) + ", ");
//            System.out.print("Department_ID : " + rs.getString(4));
//            System.out.println();
//        }
//
//        /* Drop table */
//        String DropTableSQL = "DROP TABLE student_table";
//        st.executeUpdate(DropTableSQL);
//
//        preparedStmt.close();
//        st.close();
//        rs.close();