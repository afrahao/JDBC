import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;



public class JDBCConnect{	
	public static Connection getConnection() throws SQLException 
	{
		//String DRIVER = "com.mysql.jdbc.Driver";
        Connection conn = null;
       
        try (FileInputStream f = new FileInputStream("db.properties"))
        {
 
            // load the properties file
            Properties pros = new Properties();
            pros.load(f);
 
            // assign db parameters
            String url = pros.getProperty("url");
            String user = pros.getProperty("user");
            String password = pros.getProperty("password");
            // create a connection to the database
            try 
            {
            	//¼ÓÔØMYSQL JDBCÇý¶¯³ÌÐò
      	      	Class.forName("com.mysql.jdbc.Driver");   
      	      	System.out.println("Success loading Mysql Driver!");
      	    }
      	    catch (Exception e) 
            {
      	    	System.out.print("Error loading Mysql Driver!");
      	    	e.printStackTrace();
      	    }
            conn = DriverManager.getConnection(url, user, password);
            System.out.println("Success connect Mysql server!");
        }
        catch (IOException e)
        {
            System.out.println(e.getMessage());
        }
        return conn;
    }
}