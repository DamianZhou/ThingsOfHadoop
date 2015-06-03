package ThriftJava;

//from external dependencies

import java.util.List;

import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

//from ImpalaConnect jar
import com.cloudera.impala.thrift.*;
import com.cloudera.beeswax.api.*;

public class JavaThriftClint {

	public static void main(String[] args) {

		try {
			//open connection
			TSocket transport = new TSocket("192.168.129.63", 21000);
			transport.open();
			TProtocol protocol = new TBinaryProtocol(transport);
			//connect to client
			ImpalaService.Client client = new ImpalaService.Client(protocol);
			client.PingImpalaService();

			
			//send the query            
			Query query = new Query();
			String database = "stock";
			String tablename = "stockpar";
			String querySQL = " SELECT * FROM "+tablename+" where stock='SH600368' limit 10";
			
			query.setQuery(" use "+database); // switch database
			client.query(query);
			
			//fetch the results
			query.setQuery(querySQL);

			//fetch the results
			QueryHandle handle = client.query(query);
			Results results = client.fetch(handle,false,100);
			
//			results.columns
			List<String> data = results.data; 	//默认转换为字符串
			
			for(int i=0;i<data.size();i++) {
				System.out.println(data.get(i));
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}

}
