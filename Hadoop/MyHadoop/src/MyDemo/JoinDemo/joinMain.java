package MyDemo.JoinDemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.tools.rumen.JobBuilder;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import MyDemo.JoinDemo.joinMapper;
import MyDemo.JoinDemo.joinReducer;

/**
 * 
 * @author DamianZhou
 * @see http://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Example:_WordCount_v1.0
 */
public class joinMain extends Configured implements Tool{

	public static void main(String[] args) throws Exception{

		// hdfs://192.168.129.63:9000/test/damian/joindemo/in

		//	 /test/damian/joindemo/in/student_class_info.txt
		//	 /test/damian/joindemo/in/student_info.txt

		if (args.length < 2) {
			System.err.println("Usage: joinMain <in> [<in>...] <out>");
		}
		
		int exitCode = ToolRunner.run(new joinMain(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();

		Job job = Job.getInstance(conf, "MRjoinDemo");

		job.setJarByClass(joinMain.class);

		//设置 Map&Reduce
		job.setMapperClass(joinMapper.class);
		job.setReducerClass(joinReducer.class);

		//Map输出的 key&value 类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		//Reduce输出的 key&value 类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		//设置输出文件格式
		//		job.setOutputFormatClass(); 

		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}


}
