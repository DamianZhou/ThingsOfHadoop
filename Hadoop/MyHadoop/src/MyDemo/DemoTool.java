package MyDemo;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.examples.WordCount;
import org.apache.hadoop.examples.WordCount.IntSumReducer;
import org.apache.hadoop.examples.WordCount.TokenizerMapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * @author DamianZhou
 * 
 * 官方说明：
 * A tool interface that supports handling of generic command-line options.
 * Tool, is the standard for any Map-Reduce tool/application. 
 * The tool/application should delegate the handling of standard command-line options 
 * to ToolRunner.run(Tool, String[]) and only handle its custom arguments.
 *
 * @see https://hadoop.apache.org/docs/current/api/org/apache/hadoop/util/Tool.html
 */
public class DemoTool extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		// Let ToolRunner handle generic command-line options 

		if (args.length < 2) {
			//			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			String time = new SimpleDateFormat("yyyy-MM-ddHHmmss").format(new Date());
			args=new String[2];
			args[0]="hdfs://192.168.129.63:9000/tmp_data/testin";
			args[1]="hdfs://192.168.129.63:9000/tmp_data/testout"+time;
		}
		int res = ToolRunner.run(new Configuration(), new DemoTool(), args);

		System.exit(res);

	}

	@Override
	public int run(String[] args) throws Exception {
		// Configuration processed by ToolRunner
		Configuration conf = getConf();

		//		conf.set("fs.default.name", "hdfs://192.168.129.63:9000");
		//		conf.set("hadoop.job.user", "admin");
		//		conf.set("mapreduce.framework.name", "yarn");
		//		//        conf.set("mapreduce.jobtracker.address", "192.168.1.100:9001");
		//		conf.set("yarn.resourcemanager.hostname", "192.168.129.63");
		//		conf.set("yarn.resourcemanager.admin.address", "192.168.129.63:8033");
		//		conf.set("yarn.resourcemanager.address", "192.168.129.63:8032");
		//		conf.set("yarn.resourcemanager.resource-tracker.address", "192.168.129.63:8036");
		//		conf.set("yarn.resourcemanager.scheduler.address", "192.168.129.63:8030");


		Job job = new Job(conf, "Hadoop Tool Demo");

		job.setJarByClass(WordCount.class);

		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);

		job.setOutputKeyClass(Text.class);//key类型
		job.setOutputValueClass(IntWritable.class);//value类型

		//前N-1个全是输入路径
		for (int i = 0; i < args.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(args[i]));
		}
		//最后一个是输出路径
		FileOutputFormat.setOutputPath(job,new Path(args[args.length - 1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
