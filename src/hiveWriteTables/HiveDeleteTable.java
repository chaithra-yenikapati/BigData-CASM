import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class HiveDeleteTable {
	  private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    
    /**
     * @param args
     * @throws SQLException
     */
    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        //connecting to the hive by giving the parameters as jdbc:hive2://<host>:<port>", "<user>", "<password>
        Connection con = DriverManager.getConnection("jdbc:hive2://quickstart:10000/default", "cloudera", "");
        Statement stmt = con.createStatement();
        for(int i=0;i<30;i++){
            String tableName = "marketcap"+i;
            stmt.execute("drop table if exists " + tableName);
        }
            
            String sql = "show tables";
            System.out.println("Running: " + sql);
            ResultSet res = stmt.executeQuery(sql);
            while (res.next()) {
              System.out.println(res.getString(1));
        }
    }
}