package org.apache.hadoop.examples;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;

public class EWordCount {

	/*
	 * ���������������map���������������<key, value>������������������������������������
	 * Map������������������org.apache.hadoop.mapreduce������Mapper������������������map���������
	 * ���������map������������������������key������value������������������������������
	 * ���������������map���������value���������������������������������������������������������������������������������key������������������������������������������������������������������������
	 * ������StringTokenizer���������������������������������������������
	 * ���������<word,1>������map������������������������������������������������MapReduce��������������� ������������������������ Tokenizer������������������
	 */
	public static class TokenizerMapper extends
	Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		/*
		 * ������Mapper���������map������
		 */
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			//System.out.println(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());// ���������������������������������������
				context.write(word, one);
			}
		}
	}

	/*
	 * ���������������reduce���������������������������������������������reduce���������������������map������������
	 * Reduce������������������org.apache.hadoop.mapreduce������Reducer������������������reduce���������
	 * Map������������<key,values>���key���������������������values������������������������������������������������Map���������������Reduce������������
	 * ������reduce������������������values���������������������������������������������������
	 */
	public static class IntSumReducer extends
	Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {

		UserGroupInformation ugi = UserGroupInformation
				.createRemoteUser("root");

		/**
		 * ������������������
		 */
		final File jarFile = EJob.createTempJar("bin");
		ClassLoader classLoader = EJob.getClassLoader();
		Thread.currentThread().setContextClassLoader(classLoader);

		try {
			ugi.doAs(new PrivilegedExceptionAction<Void>() {

				public Void run() throws Exception {
					/**
					 * ������hadoop������������
					 */
					Configuration conf = new Configuration(true);
					conf.set("fs.default.name", "hdfs://192.168.129.63:9000");
					conf.set("hadoop.job.user", "hadoop");
					conf.set("mapreduce.framework.name", "yarn");
					//        conf.set("mapreduce.jobtracker.address", "192.168.1.100:9001");
					conf.set("yarn.resourcemanager.hostname", "192.168.129.63");
					conf.set("yarn.resourcemanager.admin.address", "192.168.129.63:8033");
					conf.set("yarn.resourcemanager.address", "192.168.129.63:8032");
					conf.set("yarn.resourcemanager.resource-tracker.address", "192.168.129.63:8036");
					conf.set("yarn.resourcemanager.scheduler.address", "192.168.129.63:8030");

					String[] otherArgs = new String[2];
					otherArgs[0] = "hdfs://192.168.129.63:9000/tmp_data/testin";//������������������������������������������������������
					String time = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
					otherArgs[1] = "hdfs://192.168.129.63:9000/tmp_data/testout" + time;//���������������������������������������������������������������������������������������������������������������

					/*
					 * setJobName()������������������Job������Job���������������������������������������������Job���
					 * ���������JobTracker���Tasktracker������������������������������
					 */
					Job job = new Job(conf, "word count");
					job.setJarByClass(EWordCount.class);

					((JobConf) job.getConfiguration()).setJar(jarFile.toString());//������������������������������������������eclipse���������������mapreduce���������������������java������������jar������������������������������������������������������������������������������

					// job.setMaxMapAttempts(100);//���������������������������map���������������������������������������������������������������map������
					// job.setNumReduceTasks(5);//������reduce���������������������������������������

					/*
					 * Job���������Map���������������Combiner������������������������������Reduce���������������������������������
					 * ���������Reduce������������Map������������������������������������������������������������������������
					 */
					job.setMapperClass(TokenizerMapper.class);// ���������������������map������
					job.setCombinerClass(IntSumReducer.class);// ������������������map������������������������������������������������������������������
					job.setReducerClass(IntSumReducer.class);// ���������������������reduce������

					/*
					 * ������������Job������������<key,value>������key���value������������������������������<������,������>���
					 * ������key���������"Text"������������������Java���String������
					 * ���Value���������"IntWritable"������������Java������int���������
					 */
					job.setOutputKeyClass(Text.class);
					job.setOutputValueClass(IntWritable.class);

					/*
					 * ���������������������������������������������������������������
					 * ���������������������������������������������split���������������split���������<key,value>������������������������������map���������������
					 * ���������������split���������������������������hdfs������������������
					 * ���������64M���������������split������������������������������hdfs���������������������������������������������������������������������������������������
					 * ������������TextInputFormat���������������������������������������������������������
					 */
					System.out.println("Job start!");
					FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

					/*
					 * ������������������������ ������������TextOutputFormat���������������������������������������������������������������������������������������
					 */
					FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

					/*
					 * ������������������������������������
					 */
					if (job.waitForCompletion(true)) {
						System.out.println("ok!");
					} else {
						System.out.println("error!");
						System.exit(0);
					}
					return null;

				}
			});
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
}