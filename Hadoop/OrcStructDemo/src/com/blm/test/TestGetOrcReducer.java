package com.blm.test;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.blm.orc.OrcStruct;

public class TestGetOrcReducer extends Reducer<Text, IntWritable, Text, IntWritable>  {

	public IntWritable count = new IntWritable(0);   //出现次数
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		int sum = 0;    //统计总数
		while(values.iterator().hasNext()){
			sum+=values.iterator().next().get();
		}
		
		count.set(sum);
		context.write(key, count);   //输出<key,value>   <is,2>
	
	}	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
