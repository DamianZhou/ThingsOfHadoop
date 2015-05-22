package MyDemo.SubSortDemo;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class SubSortPartitioner extends HashPartitioner<Text	, NullWritable> {

	@Override
	public int getPartition(Text key, NullWritable value, int numReduceTasks) {
		
		//自定义字段的分区
		return (key.toString().split(",").hashCode() & Integer.MAX_VALUE) & numReduceTasks;
		
		//		return super.getPartition(key, value, numReduceTasks);
	}


}
