package com.blm.test;


import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;





import com.blm.orc.OrcFile;
import com.blm.orc.OrcNewInputFormat;
import com.blm.orc.OrcNewInputFormat.OrcRecordReader;
import com.blm.orc.OrcNewOutputFormat;
import com.blm.orc.OrcStruct;
import com.blm.orc.Reader;




/**
 * @author thinkpad
 *
 */
public class OrcTest {
	
	String time = new SimpleDateFormat("yyyy-MM-ddHHmmss").format(new Date());
	
	/**
	 * 遍历指定文件一定范围内的数据
	 */
	public void readOrcFile() throws IOException, InterruptedException  {
		String input = "hdfs://192.168.129.63:9000/test/tout.orc/part-r-00000";
		Configuration conf = new Configuration(); 
		Path fileIn = new Path(input);
		Reader r = OrcFile.createReader(FileSystem.get(URI.create(input), conf), fileIn);
		
		OrcRecordReader reader = new OrcRecordReader(r, conf, 0, 753); //0-300表示orcfile长度
		if ( reader!=null ) {
			//获得文件的列数
			System.out.println("========record counts : " + reader.getNumColumns()); 
		}
		while( reader.nextKeyValue() ) { 
			OrcStruct data = reader.getCurrentValue(); 
			System.out.println("fields: " + data.getNumFields());
			for(int i = 0; i < data.getNumFields(); i++){ 
				System.out.println("============" +i+ data.getFieldValue(i)); 
			}
		}
	}
	
	
	/**
	 * 生成指定路径的orc文件
	 */
	public void createOrcFile() throws IOException, InterruptedException {
//		String input = "/test/orcOut.orc"; 
//		Configuration conf = new Configuration(); 
//		Path fileIn = new Path(input);	
//		
//		for( int i = 0; i < 1000; i++ ) {
//			
//		}
		
		
	}
	
	
	public void testTextMR() throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "testTextMR");
		
//		FileSystem fs = FileSystem.get(conf);
//		if ( fs.exists(new Path("hdfs://192.168.129.63:9000/test/ttt2.txt")) ) {
//			fs.delete(new Path("hdfs://192.168.129.63:9000/test/ttt2.txt"), true);
//		}
		
		job.setJarByClass(OrcTest.class);
		//配置map和reduce
		job.setMapperClass(TestMapper.class);
		job.setReducerClass(TestReducer.class);
		//配置输出的<key,value>
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//配置inputformat，outputformat
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		//配置输入输出路径
		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.129.63:9000/test/ttt1.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.129.63:9000/test/tttout"+time));
		System.exit(job.waitForCompletion(true) ? 0 : 1);  //执行job
		System.out.println("ok");
		
	}
	
	
	public void testOrcMR() throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "word count");
		
//		FileSystem fs = FileSystem.get(conf);
//		if ( fs.exists(new Path("/test/tout.orc")) ) {
//			fs.delete(new Path("/test/tout.orc"), true);
//		}
		
		job.setJarByClass(OrcTest.class);
		//配置map和reduce
		job.setMapperClass(TestOrcMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(TestOrcReducer.class);
		//配置输出的<key,value>
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Writable.class);
		//配置inputformat，outputformat
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(OrcNewOutputFormat.class);
		//配置输入输出路径
		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.129.63:9000/test/ttt1.txt"));

        OrcNewOutputFormat.setCompressOutput(job,true);
        OrcNewOutputFormat.setOutputPath(job,new Path("hdfs://192.168.129.63:9000/test/torc"+time));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);  //执行job
		System.out.println("ok");
		
	}
	

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		OrcTest test = new OrcTest();
		try{
//			test.testTextMR();
			test.testOrcMR();
//			test.readOrcFile();
			
		} catch ( Exception ex ) {
			ex.printStackTrace();
		}
	}

}
