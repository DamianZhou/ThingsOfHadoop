package JDBCPool.c3p0.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;


public class ClientDemo {
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
		for(int i=0;i<30;i++){
			new Thread(new queryJob()).start();
		}
		System.out.println( df.get().format(new Date())+" END----> "+Thread.currentThread().getName());
	}
}

class queryJob implements Runnable{

	public void run(){
		try {
			int tail = Integer.parseInt(Thread.currentThread().getName().substring(7));
			
			if((tail & 0x01) ==1){
				try {
					Thread.sleep(tail*60000); //暂停tail分钟
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
			Connection conn = C3P0ConnentionProvider.getConnection();

			//正式获取数据
			PreparedStatement pst = conn.prepareStatement(
					" SELECT * FROM stockpar where stock='SH6003"+tail+"' " // limit 2 "
					); //为每个线程提供单独的股票ID
			ResultSet rs = pst.executeQuery();

			// impala 不支持 rs.last()
			if(rs.next()){
				System.out.println( ClientDemo.df.get().format(new Date())+" ----> "+Thread.currentThread().getName()+" ----> "+rs.getString(1));
			}


			rs.close();
			pst.close();
			conn.close();
		} catch (SQLException e) {
			System.out.println(ClientDemo.df.get().format(new Date())+" ----> "+Thread.currentThread().getName());
			e.printStackTrace();
		}

	}
}
