package com.blm.test;

import java.io.IOException;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import com.blm.orc.OrcStruct;

/**
 * 读取ORC文件的 Map类
 * @author DamianZhou
 *
 */
public class TestGetOrcMapper extends Mapper<NullWritable, Writable, Text, IntWritable>   {

	//	public Random random = new Random(100);

	protected void map(NullWritable key, Writable value, Context context) 	throws IOException, InterruptedException {
		OrcStruct orcdata=(OrcStruct) value;
		Text t_key= new Text(orcdata.getFieldValue(0).toString());
		//		int t_value= Integer.parseInt(orcdata.getFieldValue(1).toString()); //创建文件时候产生的随机数

		context.write( 	t_key, 	new IntWritable(1));            // 写入 <word，1>
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
