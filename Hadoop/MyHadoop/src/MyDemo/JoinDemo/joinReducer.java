package MyDemo.JoinDemo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class joinReducer extends Reducer<Text, Text, Text, Text> {
	
	protected static Logger logger = Logger.getLogger(joinReducer.class);

	public static final String LEFT_FILENAME="student_info.txt";
	public static final String RIGHT_FILENAME="student_class_info.txt";
	public static final String LEFT_FILENAME_FLAG="l";
	public static final String RIGHT_FILENAME_FLAG="r";
	
	@Override
	protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
//		super.reduce(key, values, context); //一定不能保留 super！！！！否则自己的逻辑无法被执行！！！
		
		Iterator<Text> iterator = values.iterator();
		
		String studentName = ""; //学生姓名
		List<String> studentClassNames = new ArrayList<>();//学生所选的课程
		
		
		while(iterator.hasNext()){
			String[] infos = iterator.next().toString().split(",");
			logger.info("------------------------->"+infos[0]+","+infos[1]);
			
			if(infos[1].equals(LEFT_FILENAME_FLAG)){
				studentName = infos[0];
			}else if(infos[1].equals(RIGHT_FILENAME_FLAG)){
				studentClassNames.add(infos[0]);
			}
		}//while
		
		for(int i=0;i<studentClassNames.size();i++){
			context.write(new Text(studentName), new Text(studentClassNames.get(i)));
		}
	}
}
