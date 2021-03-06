package JDBCPool.c3p0.test;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.log4j.Logger;


import com.mchange.v2.c3p0.DataSources;
import com.mchange.v2.c3p0.PooledDataSource;

/**
 * c3p0连接池管理类
 * @author ICE
 *
 */
public class C3P0ConnentionProvider {
	
	protected static Logger logger = Logger.getLogger(C3P0ConnentionProvider.class);

	private static final String JDBC_DRIVER = "driverClass";
	private static final String JDBC_URL = "jdbcUrl";
	
//	private static DataSource ds;
	private static PooledDataSource ds;
	
	/**
	 * 初始化连接池代码块
	 */
	static{
		initDBSource();
	}
	
	/**
	 * 初始化c3p0连接池
	 */
	private static final void initDBSource(){
		Properties c3p0Pro = new Properties();
		try {
			//加载配置文件
			c3p0Pro.load(new FileInputStream("./bin/c3p0.properties"));
		} catch (Exception e) {
			e.printStackTrace();
		} 
		
		String drverClass = c3p0Pro.getProperty(JDBC_DRIVER);
		if(drverClass != null){
			try {
				//加载驱动类
				Class.forName(drverClass);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			
		}
		
		Properties jdbcpropes = new Properties();
		Properties c3propes = new Properties();
		for(Object key:c3p0Pro.keySet()){
			String skey = (String)key;
			if(skey.startsWith("c3p0.")){
				c3propes.put(skey, c3p0Pro.getProperty(skey));
			}else{
				jdbcpropes.put(skey, c3p0Pro.getProperty(skey));
			}
		}
		
		try {
			//建立连接池
			DataSource unPooled = DataSources.unpooledDataSource(c3p0Pro.getProperty(JDBC_URL),jdbcpropes);
			ds = (PooledDataSource) DataSources.pooledDataSource(unPooled,c3propes);
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 获取数据库连接对象
	 * @return 数据连接对象
	 * @throws SQLException 
	 */
	public static synchronized Connection getConnection() throws SQLException{
		logger.info("pool status before conn: "+ds.sampleThreadPoolStatus());
		logger.info("Connections: "+ds.getNumConnectionsAllUsers() + " idle: "+ds.getNumIdleConnectionsAllUsers() + " busy: "+ds.getNumBusyConnectionsAllUsers());
		
		final Connection conn = ds.getConnection();
//		conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);  //  java.sql.SQLException: [Simba][JDBC](11975) Unsupported transaction isolation level: 2.
		
		logger.info("pool status after conn: "+ds.sampleThreadPoolStatus());
		logger.info("Connections: "+ds.getNumConnectionsAllUsers() + " idle: "+ds.getNumIdleConnectionsAllUsers() + " busy: "+ds.getNumBusyConnectionsAllUsers());
		
		return conn;
	}
}

