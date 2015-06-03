package JDBCPool.dbcp.demo;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.dbcp2.PoolingDriver;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.log4j.Logger;

/**
 * DBCP连接池管理类
 * @author DamianZhou
 *
 */
public class DBCPConnentionProvider {

	protected static Logger logger = Logger.getLogger(DBCPConnentionProvider.class);

	//	private static final String JDBC_DRIVER = "driverClassName";
	//	private static final String JDBC_URL = "url";

	private static DataSource ds;

	/**
	 * 初始化连接池代码块
	 */
	static{
		initDBSource();
	}

	/**
	 * 初始化DBCP连接池
	 */
	private static final void initDBSource(){
		Properties dbcpPro = new Properties();
		try {
			//加载配置文件
			dbcpPro.load(new FileInputStream("./bin/dbcp.properties"));
		} catch (Exception e) {
			e.printStackTrace();
		} 

		//建立连接池 way1
		try {
			ds = BasicDataSourceFactory.createDataSource(dbcpPro);//注意这段代码！！
			//			printDataSourceStats(); //输出DataSource信息
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		//建立连接池 way2
		//		ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(JDBC_URL,dbcpPro);
		//		PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, null);
		//		ObjectPool<PoolableConnection> connectionPool = new GenericObjectPool<>(poolableConnectionFactory);
		//		poolableConnectionFactory.setPool(connectionPool);
		//		ds = new PoolingDataSource<>(connectionPool);

	}

	/**
	 * 显示Pool状态
	 * @throws Exception
	 */
	public static synchronized void printDriverStats() throws Exception {
		BasicDataSource bds = (BasicDataSource) ds;
		System.out.println("-------NumActive: " + bds.getNumActive());
		System.out.println("-------NumIdle: " + bds.getNumIdle());
		System.out.println("-------NumTestsPerEvictionRun: " +bds.getNumTestsPerEvictionRun());

	}

	/**
	 * 打印所有的DataSource信息
	 * @throws Exception
	 */
	public static synchronized void printDataSourceStats() throws Exception {
		BasicDataSource bds = (BasicDataSource) ds;
		System.out.println(
				" \n AbandonedLogWriter："+ bds.getAbandonedLogWriter()
				+ " \n AbandonedUsageTracking："+ bds.getAbandonedUsageTracking()
				+ " \n CacheState："+ bds.getCacheState()
				+ " \n DefaultAutoCommit："+ bds.getDefaultAutoCommit()
				+ " \n DefaultCatalog："+ bds.getDefaultCatalog()
				+ " \n DefaultQueryTimeout："+ bds.getDefaultQueryTimeout()
				+ " \n DefaultReadOnly："+ bds.getDefaultReadOnly()
				+ " \n DefaultTransactionIsolation："+ bds.getDefaultTransactionIsolation()
				+ " \n DriverClassName："+ bds.getDriverClassName()
				+ " \n EnableAutoCommitOnReturn："+ bds.getEnableAutoCommitOnReturn()
				+ " \n EvictionPolicyClassName："+ bds.getEvictionPolicyClassName()
				+ " \n FastFailValidation："+ bds.getFastFailValidation()
				+ " \n InitialSize："+ bds.getInitialSize()
				+ " \n JmxName："+ bds.getJmxName()
				+ " \n Lifo："+ bds.getLifo()
				+ " \n LogAbandoned："+ bds.getLogAbandoned()
				+ " \n LogExpiredConnections："+ bds.getLogExpiredConnections()
				//				+ " \n LoginTimeout："+ bds.getLoginTimeout()
				+ " \n MaxConnLifetimeMillis："+ bds.getMaxConnLifetimeMillis()
				+ " \n MaxIdle："+ bds.getMaxIdle()
				+ " \n MaxOpenPreparedStatements："+ bds.getMaxOpenPreparedStatements()
				+ " \n MaxTotal："+ bds.getMaxTotal()
				+ " \n MaxWaitMillis："+ bds.getMaxWaitMillis()
				+ " \n MinEvictableIdleTimeMillis："+ bds.getMinEvictableIdleTimeMillis()
				+ " \n MinIdle："+ bds.getMinIdle()
				+ " \n NumActive："+ bds.getNumActive()
				+ " \n NumIdle："+ bds.getNumIdle()
				+ " \n NumTestsPerEvictionRun："+ bds.getNumTestsPerEvictionRun()
				+ " \n Password："+ bds.getPassword()
				+ " \n RemoveAbandonedOnBorrow："+ bds.getRemoveAbandonedOnBorrow()
				+ " \n RemoveAbandonedOnMaintenance："+ bds.getRemoveAbandonedOnMaintenance()
				+ " \n RemoveAbandonedTimeout："+ bds.getRemoveAbandonedTimeout()
				+ " \n RollbackOnReturn："+ bds.getRollbackOnReturn()
				+ " \n SoftMinEvictableIdleTimeMillis："+ bds.getSoftMinEvictableIdleTimeMillis()
				+ " \n TestOnBorrow："+ bds.getTestOnBorrow()
				+ " \n TestOnCreate："+ bds.getTestOnCreate()
				+ " \n TestOnReturn："+ bds.getTestOnReturn()
				+ " \n TestWhileIdle："+ bds.getTestWhileIdle()
				+ " \n TimeBetweenEvictionRunsMillis："+ bds.getTimeBetweenEvictionRunsMillis()
				+ " \n Url："+ bds.getUrl()
				+ " \n Username："+ bds.getUsername()
				+ " \n ValidationQuery："+ bds.getValidationQuery()
				+ " \n ValidationQueryTimeout："+ bds.getValidationQueryTimeout()
				);
	}

	/**
	 * 关闭 Pool
	 * @throws Exception
	 */
	public static void shutdownDriver() throws Exception {
		BasicDataSource bds = (BasicDataSource) ds;
		bds.close();
	}

	/**
	 * 获取数据库连接对象
	 * @return 数据连接对象
	 * @throws SQLException 
	 */
	public static synchronized Connection getConnection() throws SQLException{
		//		logger.info("pool status before conn: "+ds.sampleThreadPoolStatus());
		final Connection conn = ds.getConnection();
		return conn;
	}
}

