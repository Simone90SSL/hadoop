import java.io.*;
import java.util.*;
import java.text.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;


public class ClusterCoefficient {

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
			}
		}

		Path input = new Path(otherArgs.get(0));
		Path output =new Path(otherArgs.get(1));
		
		Job job = Job.getInstance(conf);
        job.setJarByClass(ClusterCoefficient.class);
        job.setJobName("ClusterCoefficient");
        
	    FileInputFormat.addInputPath(job, input);
	    FileOutputFormat.setOutputPath(job, output);

	    job.setMapperClass(MyMapper.class);
	    job.setReducerClass(MyReducer.class);

	    job.setInputFormatClass(TextInputFormat.class);
	    
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);
  
	    job.waitForCompletion(true);
        
        // Second job
        job = Job.getInstance();
        job.setJarByClass(ClusterCoefficient.class);
        job.setJobName("Compute_CC");
        
	    FileInputFormat.addInputPath(job, output);
	    FileOutputFormat.setOutputPath(job, new Path(output+"_CC"));

	    job.setMapperClass(ComputeCCMapper.class);
	    job.setReducerClass(ComputeCCReducer.class);

	    job.setInputFormatClass(TextInputFormat.class);
        
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
	    
        job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);    

	    job.waitForCompletion(true);
	}
		       
	
	public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
        
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String edgeString = value.toString();
            String[] edgeArray = edgeString.split("\\s+");
            context.write(new IntWritable(Integer.parseInt(edgeArray[0])), new IntWritable(Integer.parseInt(edgeArray[1])));
            context.write(new IntWritable(Integer.parseInt(edgeArray[1])), new IntWritable(Integer.parseInt(edgeArray[0])));
       	}
	}
	
	public static class MyReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text>{

		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			
			HashSet<Integer> setOfNodes = new HashSet<Integer>();
			HashSet<Integer> tempSetOfNodes = new HashSet<Integer>();
            for (IntWritable node : values) {	
				setOfNodes.add(node.get());
				tempSetOfNodes.add(node.get());
			}
			
			for(Integer node: setOfNodes){
				tempSetOfNodes.remove(node);
				context.write(new IntWritable(node), new Text(key.get()+":"+getStringFromSet(tempSetOfNodes)));
				tempSetOfNodes.add(node);			
			}
		}
		private static String getStringFromSet(HashSet<Integer> set){
			String res = "";
			
			if(set.size() == 0)
				return "";
				
			Iterator<Integer> it = set.iterator();
			res += it.next();
			while(it.hasNext()){
				res+=","+it.next();
			}
			
			return res;
		}
	}
    
    public static class ComputeCCMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
		private final static IntWritable one = new IntWritable(1);
        
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
            
        	String valueStr = value.toString();
        	String[] valueArr = valueStr.split("\t");
            context.write(new IntWritable(Integer.parseInt(valueArr[0])), new Text(valueArr[1]));
		}
	}
	
	public static class ComputeCCReducer extends Reducer<IntWritable, Text, IntWritable, Text>{

		private static final Text ZERO = new Text("0.00");
		
		private static NumberFormat formatter = new DecimalFormat("#0.00");
       @Override
		protected void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			HashMap<Integer, HashSet<Integer>> mapOfSet = new HashMap<Integer, HashSet<Integer>>();
			HashSet<Integer> setOfEdge;
			Integer tempkey = null;
			String setOfEdgesString;
			String[] nodeArray;
			for(Text setOfEdges: values){
				setOfEdge = new HashSet<Integer>();
				setOfEdgesString = setOfEdges.toString();
				tempkey = Integer.parseInt(setOfEdgesString.substring(0, setOfEdgesString.indexOf(":")));
				nodeArray = setOfEdgesString.substring(setOfEdgesString.indexOf(":")+1).split(",");
				for(String node: nodeArray){
					if(node!=null && !node.trim().equals(""))
						setOfEdge.add(Integer.parseInt(node));
				}
				mapOfSet.put(tempkey, setOfEdge);
			}	
			
			
			
			int n = mapOfSet.size();		// number of neighbours of key
			int denominator = -1;
			if(n<2){
				context.write(key, ZERO);
				return;
			}else if(n==2){
				denominator = 1;
			}else{
				denominator = n*(n-1)/2;
			}
			
			HashSet<Integer> currentSet = new HashSet<Integer>();
			Integer currValue = null;
			int numerator = 0;
			Integer keySet;
			while(mapOfSet.size()>1){
				keySet = mapOfSet.keySet().iterator().next();
				currentSet = mapOfSet.get(keySet);
				Iterator<Integer> currenIter = currentSet.iterator();
				while(currenIter.hasNext()){
					currValue = currenIter.next();
					if(mapOfSet.get(currValue)!=null)
						numerator++;
				}
				mapOfSet.remove(keySet);
			} 
			context.write(key, new Text(formatter.format((double)numerator/(double)denominator)));
		}
	}
}
