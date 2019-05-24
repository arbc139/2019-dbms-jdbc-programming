import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
  private static final String DB_DRIVER = "org.postgresql.Driver";
  public static final Logger LOG = Logger.getLogger("PROJECT 3");

  public static void main(String args[]) throws ClassNotFoundException {
    Class.forName(DB_DRIVER);
    try {
      Connection conn = getConnection("./connection.txt");
    } catch (SQLException e) {
      LOG.log(Level.SEVERE, "Connection Failed");
      throw new RuntimeException(e);
    }
  }

  private static Connection getConnection(String path) throws SQLException {
    FileParser txtParser = new FileParser();
    try {
      txtParser.open(path);
    } catch (FileNotFoundException e) {
      LOG.log(Level.SEVERE, "'connection.txt' is not exist.");
      throw new RuntimeException(e);
    }
    Map<String, String> connectionInfo = txtParser.parseTxt();
    txtParser.close();
    for (Map.Entry<String, String> entry : connectionInfo.entrySet()) {
      System.out.println(String.format("Key: %s, Value: %s", entry.getKey(), entry.getValue()));
    }

    String connectionUrl = String.format(
        "jdbc:postgresql://%s/%s", connectionInfo.get("IP"), connectionInfo.get("DB_NAME"));

    Properties props = new Properties();
    props.setProperty("user", connectionInfo.get("ID"));
    props.setProperty("password", connectionInfo.get("PW"));
    props.setProperty("currentSchema", connectionInfo.get("SCHEMA_NAME"));

    return DriverManager.getConnection(connectionUrl, props);
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