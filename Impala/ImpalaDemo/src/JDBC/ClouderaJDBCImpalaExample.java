package JDBC;

import java.sql.*;

class ClouderaJDBCImpalaExample {
	//Define a string as the fully qualified class name (FQCN) of the desired JDBC driver
	static String JDBCDriver = "com.cloudera.impala.jdbc41.Driver"; 
	
	// Define a string as the connection URL
//	static String ConnectionURL = "jdbc:impala://192.168.129.63:21050"; 
	static String ConnectionURL = "jdbc:impala://192.168.129.64:21050/stock"; //股票数据
	
	public static void main(String[] args) {
		Connection con = null; 
		Statement stmt = null; 
		ResultSet rs = null;
		String tablename = "stockpar";
		
		// Define the SQL statement for a query
		String query = "SELECT count(1) FROM "+tablename+" where stock='SH600368' ";
		try {
			System.out.println("Registering..." );
			//Register the driver using the class name
			Class.forName(JDBCDriver);

			System.out.println("Getting Connection..." );
			//Establish a connection using the connection URL
			con = DriverManager.getConnection(ConnectionURL);
			
			System.out.println("Getting stmt..." );
			//Create a Statement object for sending SQL statements to the database
			stmt = con.createStatement();
			
			System.out.println("Querying..." );
			// Execute the SQL statement
			rs = stmt.executeQuery(query);
			
			while (rs.next()) {
//				System.out.println(String.valueOf(rs.getInt(1)) + "\t" 	+ rs.getString(2)+ "\t" 	+ rs.getInt(3)+ "\t" + rs.getString(4));
				System.out.println(String.valueOf(rs.getInt(1)));
			}
					
			System.out.println("---The end---");
		} catch (SQLException se) {
			//Handle errors encountered during interaction with the data source
			se.printStackTrace();
		} catch (Exception e) {
			//Handle other errors e.printStackTrace();
			e.printStackTrace();
		} finally {
			//Perform clean up
			try {
				if (rs != null) {
					rs.close();
				}
			} catch (SQLException se3) {
				se3.printStackTrace();
			} // End try
		} // End try
		
		System.exit(0);
	} // End main
} // End ClouderaJDBCImpalaExample

