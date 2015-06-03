package JDBCPool.dbcp.demo;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;



public class checkCloseDemo {

	protected static Logger logger = Logger.getLogger(checkCloseDemo.class);

	/*
	 * 线程局部变量
	 */
	public static final ThreadLocal<SimpleDateFormat> df = new ThreadLocal<SimpleDateFormat>(){
		@Override
		protected SimpleDateFormat initialValue() {
			return new SimpleDateFormat("yyyy-MM-dd hh-mm-ss:SS");
		}

	};


	public static void main(String[] args) {
//		for(int i=0;i<50;i++){
//			new Thread(new queryJob()).start();
//		}
//		System.out.println( df.get().format(new Date())+" END----> "+Thread.currentThread().getName());

				new Thread(new checkCloseQueryJob()).start();
	}
}

class checkCloseQueryJob implements Runnable{

	public void run(){
		try {
			String JDBCDriver = "com.cloudera.impala.jdbc41.Driver"; 
			String ConnectionURL = "jdbc:impala://192.168.129.65:21050/stock"; //股票数据
			Connection con = null; 
			Statement stmt = null; 
			ResultSet rs = null;
			String tablename = "stockpar";
			
			// Define the SQL statement for a query
			String query = "SELECT * FROM "+tablename+" where stock='SH600368'  limit 20";
			try {
				System.out.println("Registering..." );
				//Register the driver using the class name
				Class.forName(JDBCDriver);

				System.out.println("Getting Connection..." );
				//Establish a connection using the connection URL
				con = DriverManager.getConnection(ConnectionURL);
				
				
				//获取Connection的部分属性
				
				System.out.println(
//						"\n NetworkTimeout ------- "+con.getNetworkTimeout()+
						"\n ClientInfo            ------- "+con.getClientInfo()+
						"\n Valid                    ------- "+con.isValid(10000)+
						"\n Schema               ------- "+con.getSchema()
						);
				DatabaseMetaData dbmd = con.getMetaData();
				System.out.println(
						"\n DriverName------- "+dbmd.getDriverName()+
						"\n DriverVersion------- "+dbmd.getDriverVersion()+
						"\n MaxConnections()------- "+dbmd.getMaxConnections()+
						"\n MaxIndexLength------- "+dbmd.getMaxIndexLength()+
						"\n MaxStatementLength------- "+dbmd.getMaxStatementLength()+
						"\n MaxIndexLength------- "+dbmd.getMaxIndexLength()+
						"\n MaxRowSize------- "+dbmd.getMaxRowSize()+
						"\n MaxStatements------- "+dbmd.getMaxStatements()+
						"\nMaxStatementLength------- "+dbmd.getMaxStatementLength()
						);
				
				System.out.println("Getting stmt..." );
				//Create a Statement object for sending SQL statements to the database
				stmt = con.createStatement();
				
				System.out.println("Querying..." );
				// Execute the SQL statement
				rs = stmt.executeQuery(query);
				
				checkCloseDemo.logger.info(checkCloseDemo.df.get().format(new Date())+"all ready... \n rs="+rs+", \n pst="+stmt+" , \n conn="+con );
				
				// impala 不支持 rs.last()
				if(rs.next()){
					System.out.println( checkCloseDemo.df.get().format(new Date())+" ----> "+Thread.currentThread().getName()+" ----> "+rs.getString(1));
				}
						
				
			} catch (SQLException se) {
				//Handle errors encountered during interaction with the data source
				se.printStackTrace();
			} catch (Exception e) {
				//Handle other errors e.printStackTrace();
				e.printStackTrace();
			}

			try { if (rs != null && !rs.isClosed()) {rs.close(); System.out.println("Closing resultset");}} catch(Exception e) { }
			try { if (stmt != null && !stmt.isClosed()) {stmt.close();System.out.println("Closing Statement");} } catch(Exception e) { }
			try { if (con != null && !con.isClosed() ) {con.close(); System.out.println("Closing Connection");}} catch(Exception e) { }

			try { if (rs != null && !rs.isClosed()) {rs.close(); System.out.println("check resultset");}} catch(Exception e) { }
			try { if (stmt != null && !stmt.isClosed()) {stmt.close();System.out.println("check Statement");} } catch(Exception e) { }
			try { if (con != null && !con.isClosed() ) {con.close(); System.out.println("check Connection");}} catch(Exception e) { }
			
			Thread.sleep(60000); //睡一分钟
			
			
			System.out.println("---The end---");

		}catch (Exception e) {
			e.printStackTrace();
		}
		
	}//run
}



