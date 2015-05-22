package MyDemo.JoinDemo;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;


public class joinMapper extends Mapper<LongWritable, Text, Text, Text> {

	protected static Logger logger = Logger.getLogger(joinMapper.class);

	public static final String LEFT_FILENAME="student_info.txt";
	public static final String RIGHT_FILENAME="student_class_info.txt";
	public static final String LEFT_FILENAME_FLAG="l";
	public static final String RIGHT_FILENAME_FLAG="r";

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
	}

	@Override
	public void run(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.run(context);
	}

	@Override
	protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
		//		super.map(key, value, context); //一定不能保留 super！！！！否则自己的逻辑无法被执行！！！


		String filePath = ((FileSplit)context.getInputSplit()).getPath().toString(); //获取HDFS路径
		String fileFlag = null;
		String joinKey = null;
		String joinValue = null;

		String[] info = value.toString().split(",");

		//根据文件的来源，设置 key/value
		if(filePath.contains(LEFT_FILENAME)){
			fileFlag = LEFT_FILENAME_FLAG;
			if(info.length==2){
				joinKey = info[1];
				joinValue = info[0];
			}
		}else if(filePath.contains(RIGHT_FILENAME)){
			fileFlag = RIGHT_FILENAME_FLAG;
			if(info.length==2){
				joinKey=info[0];
				joinValue=info[1];
			}
		}

		logger.info("------------------------->"+joinKey+" , "+joinValue);

		//默认读取文件的操作中，会有空行被读入，引起空指针异常！！！
		if(info.length==2)
			context.write(new Text(joinKey) , new Text(joinValue + "," +fileFlag));

	}




}
