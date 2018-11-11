package pt.lucas2010.hadoop.mapreduce.datacap.analyzer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ThreadSegregationMapper extends Mapper<Object, Text, Text, Text> {
	private boolean logExceptions = false;
	
	//private final static IntWritable one = new IntWritable(1);
	private Text threadId = new Text();
	private Text nonThread = new Text("NonThread");

	private int firstSpace = 8;
	private int secondSpace = 21;
	private int thirdSpace = 23;
	private int fourthSpace = 28;
	private int minimumLength = 50;

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		try {
			String str = this.getThreadId(value);
			if(str!=null)
				this.threadId.set(str);
			context.write(str!=null ? this.threadId : this.nonThread, value);
		}
		catch(Throwable th) {
			if(this.logExceptions) {
				System.out.println(th.getMessage());
				th.printStackTrace();
			}
		}
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
