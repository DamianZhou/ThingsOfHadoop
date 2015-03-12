/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.examples;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

	/**
	 * Map处理
	 * @author DamianZhou
	 *
	 */
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	/**
	 * Reduce处理
	 * @author DamianZhou
	 *
	 */
	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,Context context ) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(final String[] args) throws Exception {


		UserGroupInformation ugi = UserGroupInformation.createRemoteUser("admin");

		try {
			ugi.doAs(new PrivilegedExceptionAction<Void>() {

				public Void run() throws Exception {
					Configuration conf = new Configuration();
					
//					conf.set("fs.default.name", "hdfs://192.168.129.63:9000");
//					conf.set("hadoop.job.user", "admin");
//					conf.set("mapreduce.framework.name", "yarn");
//					//        conf.set("mapreduce.jobtracker.address", "192.168.1.100:9001");
//					conf.set("yarn.resourcemanager.hostname", "192.168.129.63");
//					conf.set("yarn.resourcemanager.admin.address", "192.168.129.63:8033");
//					conf.set("yarn.resourcemanager.address", "192.168.129.63:8032");
//					conf.set("yarn.resourcemanager.resource-tracker.address", "192.168.129.63:8036");
//					conf.set("yarn.resourcemanager.scheduler.address", "192.168.129.63:8030");

					String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//					if (otherArgs.length < 2) {
//						System.err.println("Usage: wordcount <in> [<in>...] <out>");
//						System.exit(2);		
						String time = new SimpleDateFormat("yyyy-MM-ddHHmmss").format(new Date());
						otherArgs[0]="hdfs://192.168.129.63:9000/tmp_data/testin";
						otherArgs[1]="hdfs://192.168.129.63:9000/tmp_data/testout"+time;
//						otherArgs[0]="tmp_data/testin";
//						otherArgs[1]="tmp_data/testout"+time;
//					}

					Job job = new Job(conf, "Damian's WordCount");

					job.setJarByClass(WordCount.class);

					job.setMapperClass(TokenizerMapper.class);
					job.setCombinerClass(IntSumReducer.class);
					job.setReducerClass(IntSumReducer.class);

					job.setOutputKeyClass(Text.class);//key类型
					job.setOutputValueClass(IntWritable.class);//value类型

					//前N-1个全是输入路径
					for (int i = 0; i < otherArgs.length - 1; ++i) {
						FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
					}
					//最后一个是输出路径
					FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length - 1]));

					System.exit(job.waitForCompletion(true) ? 0 : 1);

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

		
		//---------原代码
		//		Configuration conf = new Configuration();
		//		
		//		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		//		if (otherArgs.length < 2) {
		//			System.err.println("Usage: wordcount <in> [<in>...] <out>");
		//			System.exit(2);					
		//		}
		//		
		//		Job job = new Job(conf, "word count");
		//		
		//		job.setJarByClass(WordCount.class);
		//		
		//		job.setMapperClass(TokenizerMapper.class);
		//		job.setCombinerClass(IntSumReducer.class);
		//		job.setReducerClass(IntSumReducer.class);
		//		
		//		job.setOutputKeyClass(Text.class);//key类型
		//		job.setOutputValueClass(IntWritable.class);//value类型
		//		
		//		//前N-1个全是输入路径
		//		for (int i = 0; i < otherArgs.length - 1; ++i) {
		//			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		//		}
		//		//最后一个是输出路径
		//		FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length - 1]));
		//		
		//		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
