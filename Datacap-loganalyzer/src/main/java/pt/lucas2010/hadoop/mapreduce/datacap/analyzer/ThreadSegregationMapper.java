package pt.lucas2010.hadoop.mapreduce.datacap.analyzer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ThreadSegregationMapper extends Mapper<Object, Text, Text, Text> {

	//private final static IntWritable one = new IntWritable(1);
	private Text threadId = new Text();

	private int firstSpace = 10;
	private int secondSpace = 20;
	private int thirdSpace = 30;
	private int fourthSpace = 40;
	private int minimumLength = 50;

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String str = this.getThreadId(value);
		this.threadId.set(str);
		context.write(threadId, value);
	}
	
	private String getThreadId(Text value) {
		if (value == null)
			return null;
		String str = value.toString();
		if (str.length() < minimumLength)
			return null;


		if (str.charAt(firstSpace) != ' ')
			return null;
		if (str.charAt(secondSpace) != ' ')
			return null;
		if (str.charAt(thirdSpace) != ' ')
			return null;
		if (str.charAt(fourthSpace) != ' ')
			return null;
		
		return str.substring(thirdSpace+1, fourthSpace);
	}
}
