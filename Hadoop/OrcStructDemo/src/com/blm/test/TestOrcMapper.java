package com.blm.test;

import java.io.IOException;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class TestOrcMapper extends Mapper<Object, Text, Text, IntWritable>   {

	public Text keyText = new Text("key");  //string keyText = "key";
	//	public IntWritable intValue = new IntWritable();
	public Random random = new Random(100);

	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		//将输入的纯文本的数据转换成String    my name is Liming
		String str = value.toString();	
		//将输入的数据按空白字符分割
		StringTokenizer stringTokenizer = new StringTokenizer(str);   //分割
		while(stringTokenizer.hasMoreTokens()){
			keyText.set(stringTokenizer.nextToken());    //key
			//			IntWritable intValue = new IntWritable();
			context.write(keyText, new IntWritable(random.nextInt()));            //输出<key,value>  <my,1>      
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
