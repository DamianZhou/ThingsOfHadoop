package MyDemo.SubSortDemoII;

import org.apache.hadoop.io.*;

/**
 * 分组 Group 比较器，控制不同记录合并为一个文件时的顺序
 * 
 * Grouping occurs when the 【reduce phase】 is streaming map output records from local disk. 
 * 
 * Grouping is the process by which you can specify how records are combined 
 * to form one logical sequence of records for a reducer invocation.
 * 
 * When you're at the grouping stage, all of the records are already in secondary-sort order, 
 * and the grouping comparator needs to bundle together records with the same last name
 * 
 */
public class PersonNameComparator extends WritableComparator {

	protected PersonNameComparator() {
		super(Person.class, true);
	}

	@Override
	public int compare(WritableComparable o1, WritableComparable o2) {

		Person p1 = (Person) o1;
		Person p2 = (Person) o2;

//		return p1.getLastName().compareTo(p2.getLastName());
		return p2.getLastName().compareTo(p1.getLastName()); //效果相同，并不进行排序
	}
}
