import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;


public class Graph {

	static int printUsage() {
		System.out.println("graph [-r <reduces>] <input> <output>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	public static void main(String[] args) throws Exception {
		
		List<String> otherArgs = new ArrayList<String>();

		Configuration conf = new Configuration();
		
		for(int i=0; i < args.length; ++i) {
			try {
				if ("-r".equals(args[i])) {
					conf.setInt("mapreduce.job.reduces", Integer.parseInt(args[++i]));
				} else {
					otherArgs.add(args[i]);
				}
			} catch (NumberFormatException except) {
				System.out.println("ERROR: Integer expected instead of " + args[i]);
				System.exit(printUsage());
			} catch (ArrayIndexOutOfBoundsException except) {
				System.out.println("ERROR: Required parameter missing from " +
						args[i-1]);
				System.exit(printUsage());
			}
		}

		// Make sure there are exactly 2 parameters left.
		if (otherArgs.size() != 2) {
			System.out.println("ERROR: Wrong number of parameters: " +
					otherArgs.size() + " instead of 2.");
			System.exit(printUsage());
		}
		
		Path input = new Path(otherArgs.get(0));
		Path output =new Path(otherArgs.get(1));
		
		Job job = Job.getInstance(conf);
        job.setJarByClass(Graph.class);
        job.setJobName("Graph");
        
	    FileInputFormat.addInputPath(job, input);
	    FileOutputFormat.setOutputPath(job, output);

	    job.setMapperClass(MyMapper.class);
	    //job.setCombinerClass(MyReducer.class);
	    job.setReducerClass(MyReducer.class);

        // An InputFormat for plain text files. 
        // Files are broken into lines. Either linefeed or carriage-return are used 
        // to signal end of line. Keys are the position in the file, and values 
        // are the line of text.
	    job.setInputFormatClass(TextInputFormat.class);

		job.setMapOutputValueClass(EdgeWritable.class);

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    

	    job.waitForCompletion(true);
	   
	   
	    // Elimina duplicati
		// Create a new Job
		job = Job.getInstance();
		job.setJarByClass(Graph.class);

		// Specify various job-specific parameters     
		job.setJobName("DeleteDuplicate");

		FileInputFormat.addInputPath(job, new Path(otherArgs.get(1)+"/part-r-00000"));
		FileOutputFormat.setOutputPath(job, new Path(output+"DD"));

		job.setMapperClass(DDMapper.class);
		job.setReducerClass(DDReducer.class);
		
		job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);

		// Submit the job, then poll for progress until the job is complete
		job.waitForCompletion(true);
		
	    
	}
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, EdgeWritable>{
		private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] edge = value.toString().split(" ");
			EdgeWritable edgeWr = new EdgeWritable(Integer.parseInt(edge[0]), Integer.parseInt(edge[1]));
			System.out.println("[MAP] "+edgeWr); 
			context.write(new Text(edge[0]), edgeWr);
			context.write(new Text(edge[1]), edgeWr);
			
			}
       	}
	
	
	public static class MyReducer extends Reducer<Text, EdgeWritable, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<EdgeWritable> values, Context context)
				throws IOException, InterruptedException {
			
			Set<String> inEdge = new HashSet<String>();
			Set<String> outEdge = new HashSet<String>();
			
			//String keyString = key.toString();
			int keyString = Integer.parseInt(key.toString());
			String[] edgeArray; 
			/*
			OLD CODE
			for (Text value : values) {
					edgeArray = value.toString().split(" ");
					if(edgeArray[1].equals(keyString)){
						// This is an inEdge
						inEdge.add(edgeArray[0]);
					} else{
						outEdge.add(edgeArray[1]);
					}
			}
			*/
			for (EdgeWritable value : values) {
				System.out.println("[REDUCE] key: "+keyString+" | edge: "+value);
				if (value.getOutEdge()==keyString){
					// This is an inEdge
					inEdge.add(""+value.getInEdge());
				} else{
					outEdge.add(""+value.getOutEdge());
				}
			}
			for(String ie: inEdge){
				for(String oe: outEdge){
					context.write(new Text(ie), new Text(oe));
				}
			}
			
		}
	}
	
	public static class DDMapper extends Mapper<LongWritable, Text, Text, Text>{
		private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] valueArray = value.toString().split("\\s+");
			context.write(new Text(valueArray[0]), new Text(valueArray[1]));
			
			}
       	}
	
	
	public static class DDReducer extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			Set<String> writedEdge = new HashSet<String>();
			String valueString = "";
			for(Text value: values){
				valueString = value.toString();
				if(writedEdge.contains(valueString)){
					continue;
				}
				context.write(key, new Text(valueString));
				writedEdge.add(valueString);
			}
			
			
		}
	}
}
