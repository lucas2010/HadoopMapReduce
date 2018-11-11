package pt.lucas2010.hadoop.mapreduce.datacap.analyzer;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ThreadSegregationReducer extends Reducer<Text, Text, Text, Text> {
	
    private MultipleOutputs<Text, Text> mos;
    
    
    
    public void setup(Context context) {
         this.mos = new MultipleOutputs<Text, Text>(context);
     }
    
//    private void ensureFile(Text key) {
//        this.mos.
//    	MultipleOutputs.addNamedOutput(threadSegregatorJob, "text", TextOutputFormat.class,
//        	    LongWritable.class, Text.class);
//    	
//    }
    

    public void reduce(Text key, Iterable<Text> values, Context context
			) throws IOException, InterruptedException {
		for (Text val : values) {
			//context.write(key, val);
			this.mos.write(key, val, key.toString());
		}
	}
    
    public void cleanup(Context context) throws IOException, InterruptedException {
		this.mos.close();
    }
    
    
}
