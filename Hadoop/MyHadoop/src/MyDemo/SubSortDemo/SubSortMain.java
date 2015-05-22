package MyDemo.SubSortDemo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SubSortMain {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		// hdfs://192.168.129.63:9000/test/damian/subsortdemo/in
		
		if (args.length < 2) {
			System.err.println("Usage: joinMain <in> [<in>...] <out>");
		}

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "MRjoinDemo");

		job.setJarByClass(SubSortMain.class);

		//设置 Map&Reduce
		job.setMapperClass(SubSortMapper.class);
		job.setPartitionerClass(SubSortPartitioner.class);
		job.setSortComparatorClass(SubSortComparator.class);
		job.setReducerClass(SubSortReducer.class);
		

		//Map输出的 key&value 类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		//Reduce输出的 key&value 类型
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		//设置输出文件格式
		//		job.setOutputFormatClass(); 
		
		job.setNumReduceTasks(1); //排序的时候，需要指定为1
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
