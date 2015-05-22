package MyDemo.SubSortDemo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SubSortComparator extends WritableComparator {
	/**
	 * 构造方法
	 */
	protected SubSortComparator(){
		super(Text.class , true);
	}

	/**
	 * 此处直接比较 Map输出的 key
	 */
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		//		return super.compare(a, b);

		String[] key1 = a.toString().split(","); 
		String[] key2 = b.toString().split(",");

		//如果第一个字段不同，直接比较第一个字段
		if(Integer.parseInt(key1[0]) != Integer.parseInt(key2[0])){
			return Integer.parseInt(key1[0]) - Integer.parseInt(key2[0]);
		}else{
			//如果第一个字段相同，则比较第二个字段
			return Integer.parseInt(key1[1]) - Integer.parseInt(key2[1]);
		}

	}

}
