import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class HiveCreateTable {
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
      //connecting to the hive by giving the parameters as  
    Connection con = DriverManager.getConnection("jdbc:hive2://quickstart:10000/default", "cloudera", "");
    Statement stmt = con.createStatement();
    for(int i=0;i<30;i++){
    String tableName = "hiveNYSE"+i;
    //stmt.execute("drop table if exists " + tableName);
    stmt.execute("create external table " + tableName + "(date string, open double, close double, low double, high double, volume decimal) row format delimited fields terminated by ','");
    String filepath;
    if(i<10){
    	filepath = "/user/cloudera/final/output/part-m-0000"+i;
    	}
    else{
    	filepath = "/user/cloudera/final/output/part-m-000"+i;
    }
    // the hdfs file path where the input files are stored is given as file path.
    String sql = "load data inpath '" + filepath + "' into table " + tableName;
    System.out.println("Running: " + sql);
    stmt.execute(sql);
    /*creating new table for each company from the earlier generated table by just selecting date and that day's market capitalization as
     * closing price * volume. 
     * for simplicity, i am representing it in terms of K i.e 
     * if market cap is 10000 we are storing it as 10 which is 10K */
    stmt.execute("create table "+"marketCapNYSE"+i+" as select date, close*(volume/1000) as marketCap from "+tableName );
    }
    // show tables
    String sql = "show tables";
    System.out.println("Running: " + sql);
    ResultSet res = stmt.executeQuery(sql);
    while (res.next()) {
      System.out.println(res.getString(1));
    }
  }
}