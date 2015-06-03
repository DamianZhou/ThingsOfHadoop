package JDBCPool.dbcp.demo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;



public class dbcpClientDemo {

	protected static Logger logger = Logger.getLogger(dbcpClientDemo.class);

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

				new Thread(new queryJob()).start();
	}
}

class queryJob implements Runnable{

	public void run(){
		try {
			int tail = Integer.parseInt(Thread.currentThread().getName().substring(7));

			//						if((tail & 0x01) ==1 || tail>5){
//			try {
//				Thread.sleep((tail % 10)*60000); //暂停tail分钟
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}

			Connection conn = DBCPConnentionProvider.getConnection();
			DBCPConnentionProvider.printDriverStats(); //打印当前Pool信息

			//正式获取数据
			PreparedStatement pst = conn.prepareStatement(
					" SELECT * FROM stockpar where stock='SH60031"+tail+"' " // limit 2 "
					); //为每个线程提供单独的股票ID
			ResultSet rs = pst.executeQuery();

			// impala 不支持 rs.last()
			if(rs.next()){
				System.out.println( dbcpClientDemo.df.get().format(new Date())+" ----> "+Thread.currentThread().getName()+" ----> "+rs.getString(1));
			}


			try { if (rs != null) rs.close(); } catch(Exception e) { }
			try { if (pst != null) pst.close(); } catch(Exception e) { }
			try { if (conn != null) conn.close(); } catch(Exception e) { }

			dbcpClientDemo.logger.info(dbcpClientDemo.df.get().format(new Date())+"after close... \n rs="+rs+", \n pst="+pst+" , \n conn="+conn );

		} catch (SQLException e) {
			System.out.println(dbcpClientDemo.df.get().format(new Date())+" ----> "+Thread.currentThread().getName());
			e.printStackTrace();
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
}
