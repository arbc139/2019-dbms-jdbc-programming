import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public class PsqlConnection {
  public static String PSQL_CONNECTION_IP_KEY = "IP";
  public static String PSQL_CONNECTION_DB_NAME_KEY = "DB_NAME";
  public static String PSQL_CONNECTION_ID_KEY = "ID";
  public static String PSQL_CONNECTION_PW_KEY = "PW";
  public static String PSQL_CONNECTION_SCHEMA_NAME_KEY = "SCHEMA_NAME";

  public static PsqlConnection create(Connection conn, Map<String, String> connInfo) {
    PsqlConnection psqlConn = new PsqlConnection();
    psqlConn.rawConn = conn;
    psqlConn.info = connInfo;
    return psqlConn;
  }

  public String getBaseSchemaName() {
    return info.get(PSQL_CONNECTION_SCHEMA_NAME_KEY);
  }

  public void close() {
    try {
      rawConn.close();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }


  public Connection rawConn;
  public Map<String, String> info;
}
