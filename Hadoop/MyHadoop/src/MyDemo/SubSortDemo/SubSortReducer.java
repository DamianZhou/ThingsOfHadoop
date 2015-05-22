package MyDemo.SubSortDemo;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SubSortReducer extends Reducer<Text, Text, NullWritable, Text> {

	@Override
	protected void reduce(Text arg0, Iterable<Text> arg1,Context arg2) throws IOException, InterruptedException {
		//		super.reduce(arg0, arg1, arg2);
		for(Text v : arg1){
			arg2.write(NullWritable.get(), v);
		}
	}



}
