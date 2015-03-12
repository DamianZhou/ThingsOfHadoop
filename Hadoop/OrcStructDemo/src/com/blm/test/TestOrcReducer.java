package com.blm.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import com.blm.orc.OrcSerde;

public class TestOrcReducer extends Reducer<Text, IntWritable, NullWritable, Writable>   {

    private final OrcSerde serde = new OrcSerde();

    //Define the struct which will represent each row in the ORC file
    private final String typeString = "struct<key_word:string,random_id:int>";

    private final TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeString);
    private final ObjectInspector oip = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);	
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context)
	throws IOException, InterruptedException {
		int sum = 0;    //统计总数
		while(values.iterator().hasNext()){
			sum+=values.iterator().next().get();
		}
		System.out.print(key.toString());
		System.out.print(sum);
        List<Object> struct =new ArrayList<Object>(4);
        struct.add(0, key.toString());
        struct.add(1, sum);

        Writable row = serde.serialize(struct, oip);

		context.write(NullWritable.get(), row);   //输出<key,value>   <is,2>
	
	}	

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
