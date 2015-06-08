package MyDemo.SubSortDemoII;

import org.apache.hadoop.io.WritableComparable;

import java.io.*;

/**
 * Person is a " composite key" 复合键
 * @author DamianZhou
 *
 */
public class Person implements WritableComparable<Person> {

	private String firstName; //用于 分区和分组
	private String lastName; //用于排序

	public Person() {
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.lastName = in.readUTF();
		this.firstName = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(lastName);
		out.writeUTF(firstName);
	}

	@Override
	public int compareTo(Person other) {
		int cmp = this.lastName.compareTo(other.lastName);
		if (cmp != 0) {
			return cmp;
		}
		return this.firstName.compareTo(other.firstName);
	}

	public void set(String lastName, String firstName) {
		this.lastName = lastName;
		this.firstName = firstName;
	}
}