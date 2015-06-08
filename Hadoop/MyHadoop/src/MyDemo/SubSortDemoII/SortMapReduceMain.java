package MyDemo.SubSortDemoII;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public final class SortMapReduceMain {
	public static void main(String... args) throws Exception {
		
		// hdfs://192.168.129.63:9000/test/damian/subsortdemo/in/name
		// hdfs://192.168.129.63:9000/test/damian/subsortdemo/out/name1
		
		runSortJob(args[0], args[1]);
	}

	public static void runSortJob(String input, String output) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf);
		job.setJarByClass(SortMapReduceMain.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		//设置InputFormat ，默认将一行数据按照 \t 分为 key/value
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		//Map输出类型
		job.setMapOutputKeyClass(Person.class);
		job.setMapOutputValueClass(Text.class);

		//Reduce输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setPartitionerClass(PersonNamePartitioner.class); //分区，控制Map的输出到哪个Reduce
		job.setSortComparatorClass(PersonComparator.class);//排序，Map和Reduce都有
//		job.setGroupingComparatorClass(PersonNameComparator.class); //分组,按照 key 将相同的key放到一起。实测，无影响

		Path outputPath = new Path(output);

		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, outputPath);

		outputPath.getFileSystem(conf).delete(outputPath, true);

		job.waitForCompletion(true);
	}

	/**
	 * Mapper
	 * @author DamianZhou
	 */
	public static class Map extends Mapper<Text, Text, Person, Text> {

		private Person outputKey = new Person(); //Person是一个 组合键 <LastName , FirstName>

		@Override
		protected void map(Text lastName, Text firstName, Context context) throws IOException, InterruptedException {
			outputKey.set(lastName.toString(), firstName.toString());
			context.write(outputKey, firstName);
		}
	}

	
	/**
	 * Reducer
	 * @author DamianZhou
	 */
	public static class Reduce extends Reducer<Person, Text, Text, Text> {

		Text lastName = new Text();

		@Override
		public void reduce(Person key, Iterable<Text> values,	Context context) throws IOException, InterruptedException {
			// <LastName , FirstName>
			
			lastName.set(key.getLastName());
			
			for (Text firstName : values) {
				context.write(lastName, firstName);
			}
		}//reduce
		
	}
	
}
