package MyDemo.SubSortDemoII;

import org.apache.hadoop.io.*;

/*
 * 排序 Sorting 比较器
 * 
 * Both the map and reduce sides participate in sorting. 
 * 
 * The map-side sorting is an optimization to help make the reducer sorting more efficient. 
 * You want MapReduce to use your entire key for sorting purposes, which will order keys according to both the last name and the first name.
 * 
 * In the following example you can see the implementation of the WritableComparator, which compares users based on their last name and their first name
 */
public class PersonComparator extends WritableComparator {
	protected PersonComparator() {
		super(Person.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {

		Person p1 = (Person) w1;
		Person p2 = (Person) w2;

		// 先比较 LastName ，再比较 FirstName
		
		int cmp = p1.getLastName().compareTo(p2.getLastName());
		if (cmp != 0) {
			return cmp;
		}

		return p1.getFirstName().compareTo(p2.getFirstName());
	}
}
