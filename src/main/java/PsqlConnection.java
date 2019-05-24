import java.sql.Connection;
import java.util.Map;

public class PsqlConnection {
  public static PsqlConnection create(Connection conn, Map<String, String> connInfo) {
    PsqlConnection psqlConn = new PsqlConnection();
    psqlConn.rawConn = conn;
    psqlConn.info = connInfo;
    return psqlConn;
  }

  public Connection rawConn;
  public Map<String, String> info;
}
