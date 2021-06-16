package excercise1a;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class ex1a {
	//MAPPER CODE	
	   
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	String myvalue=value.toString();//Text - String
	String[] transactiontokens = myvalue.split(",");
	System.out.println(value);
	if (transactiontokens[5].equals("Yes")) 
	output.collect(new Text("Total Eligible for Salary Hike"),one);
	output.collect(new Text("Total Awards for the year"), new IntWritable(Integer.parseInt(transactiontokens[3]))) ;	
	if (transactiontokens[2].equals("30000"))
	output.collect(new Text("Total Awards of Employees Salary=30000"),one);	
	if (transactiontokens[4].equals("Yes")) 
		output.collect(new Text("Total Tax Paid Employees "),one);
	}	
	}

	//REDUCER CODE	
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException { //{little: {1,1}} 
	int eligiblecount=0;
	System.out.println(values);
	System.out.println(key);
	while(values.hasNext()) {
		eligiblecount += values.next().get();
	}
	output.collect(key,new IntWritable(eligiblecount));
	}
	}
		
	//DRIVER CODE
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(ex1a.class);
		conf.setJobName("Counting the number of eligible employees for pay raise ");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class); // hadoop jar jarname classpath inputfolder outputfolder
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);   
	}
	}
