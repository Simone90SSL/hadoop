import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;


public class DirectChecking {

	static int printUsage() {
		System.out.println("DirectChecking [-r <reduces>] <input> <output>");
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
        job.setJarByClass(DirectChecking.class);
        job.setJobName("DirectChecking");
        
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

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);

	    job.waitForCompletion(true);
	}
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{
		private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] edge = value.toString().split(" "); 
			context.write(new Text(edge[0]+","+edge[1]), new Text("E"));
			context.write(new Text(edge[1]+","+edge[0]), new Text("C"));
			
			}
       	}
	
	
	public static class MyReducer extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			boolean isExisted = false;
			boolean isChecked = false;
			String keyString = key.toString();
			String[] edgeArray; 
			for (Text value : values) {
					if(value.toString().equals("E")){
						isExisted = true;
					} else{
						isChecked = true;
					}
			}
			
			if(isExisted && !isChecked)
				context.write(key, new Text("Non ha un edge inverso!!!"));
		}
	}
}
