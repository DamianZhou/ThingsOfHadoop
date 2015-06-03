package JDBCPool;
import java.beans.PropertyVetoException;

import com.mchange.v2.c3p0.ComboPooledDataSource;


public class JDBCPoolDemo {

	static String JDBCDriver = "com.cloudera.impala.jdbc4.Driver"; 
	static String ConnectionURL = "jdbc:impala://192.168.129.64:21050/stock"; //股票数据


	public static void main(String[] args){
		ComboPooledDataSource cpds = new ComboPooledDataSource();
		try {
			cpds.setDriverClass(JDBCDriver);
			cpds.setJdbcUrl(ConnectionURL);
			cpds.setUser("dbuser");                                  
			cpds.setPassword("dbpassword");       
		} catch (PropertyVetoException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  

	}

}
