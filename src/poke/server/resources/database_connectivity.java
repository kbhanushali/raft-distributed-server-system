package poke.server.resources;
import java.sql.*;
public class database_connectivity {


   // JDBC driver name and database URL
   static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
   static final String DB_URL = "jdbc:mysql://localhost/";

   //  Database credentials
   static final String USER = "root";
   static final String PASS = "";
   
   private Connection getConnection() {
   Connection conn = null;
  // Statement stmt = null;
   try{
      
      Class.forName("com.mysql.jdbc.Driver");
      System.out.println("Connecting to database...");
      conn = DriverManager.getConnection(DB_URL, USER, PASS);
     // stmt = conn.createStatement();
      return conn;
      
   }catch(SQLException se){
      //Handle errors for JDBC
      se.printStackTrace();
   }catch(Exception e){
      //Handle errors for Class.forName
      e.printStackTrace();
   }
      try{
         if(conn!=null)
            conn.close();
      }catch(SQLException se){
         se.printStackTrace();
      }
	return conn;
   }
   
   public String execute_query(String query) throws SQLException{
	   Statement stmt = null;
	   Connection conn = getConnection();
	   try {
			stmt = conn.createStatement();
			stmt.executeUpdate(query);
			return "Success";
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "Failed to insert";
		}
	   finally {
		    if (stmt != null) { stmt.close();	
		    conn.close();}
		}
	   
   }
   
   public static void main(String args[]) throws SQLException{
	   database_connectivity db = new database_connectivity();
	   	String file_path = "images/"+1;
		String sql = "insert into CMPE_275.Data values (1,1,1,'"+file_path+"','')";
		System.out.println(sql);
		System.out.println(db.execute_query(sql));
   }
}


