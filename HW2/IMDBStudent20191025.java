import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class IMDBStudent20191025
{

	public static class Movie 
	{
		public String title;
		public double average;
		
		public Movie(String _title, double _average)
		{
			this.title = _title;
			this.average = _average;
		}
		public String getTitle() 
		{
			return this.title;
		}
		public double getAverage() 
		{
			return this.average;
		}
			
		public String getString()
		{
			return title + "|" + average;
		}
		
	}
	public static class MovieComparator implements Comparator<Movie> {
		public int compare(Movie x, Movie y) {
			if (x.average > y.average) return 1;
			if (x.average < y.average) return -1;
			return 0;
		}
	}

	public static void insertMovie(PriorityQueue q, String title, double average, int topK) {
		Movie movie_head = (Movie)q.peek();
		if (q.size() < topK || movie_head.average < average) {
			Movie movie = new Movie(title, average);
			q.add(movie);
			if (q.size() > topK) q.remove();
		}
	}

	public static class IMDBMapper extends Mapper<Object, Text, DoubleString, Text>
	{
		boolean fileA = true;
        	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            		String[] line = value.toString().split("::");
            		DoubleString outputKey = new DoubleString();
            		Text outputValue = new Text();
            		
            		String result = "";

            		if (fileA) {
                		String id = line[0];
                		String title = line[1];
                		String genre = line[2];

                		StringTokenizer itr = new StringTokenizer(genre, "|");
                		boolean isFantasy = false;
                		while (itr.hasMoreTokens()) {
                    			if (itr.nextToken().equals("Fantasy")) {
                        			isFantasy = true;
                        			break;
                    			}
                		}

                		if (isFantasy) {
                    			outputKey = new DoubleString(id, "A");
                    			result = title;
                    			outputValue.set(title);
                    			context.write( outputKey, outputValue );
                		}

            		} else {
                		String id = line[1];
                		String rating = line[2];

                		outputKey = new DoubleString(id, "B");
                		outputValue.set(rating);
                		result = rating;
                		context.write( outputKey, outputValue );
            		}
            		//System.out.println(result);
            		
			
		}
		protected void setup(Context context) throws IOException, InterruptedException {
            		String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
            		if ( filename.indexOf( "movies.dat" ) != -1 ) fileA = true;
            		else fileA = false;
        	}
	}

	public static class IMDBReducer extends Reducer<DoubleString,Text,Text,DoubleWritable> 
	{
		private PriorityQueue<Movie> queue;
		private Comparator<Movie> comp = new MovieComparator();
		private int topK;
		
		public void reduce(DoubleString key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			double sum = 0;
			int count = 0;
			String title = "";
			boolean isFantasy = false;
			for(Text val: values)
			{
				try {
					sum += Integer.parseInt(val.toString());
					count++;
				
				} catch (NumberFormatException ex){
					title = val.toString();
					isFantasy = true;
				}
			}
			if(isFantasy)
			{
				double avg = sum / count;
				System.out.println(title + " " + avg);
				insertMovie(queue, title, avg, topK);
			}
			
			
		}
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<Movie>(topK, comp);
		} 
		protected void cleanup(Context context) throws IOException, InterruptedException {
			while (queue.size() != 0) {
				Movie movie = (Movie)queue.remove();
				context.write(new Text(movie.getTitle()), new DoubleWritable( movie.getAverage()));
			}
		}
	}
	
	public static class DoubleString implements WritableComparable
	{
		String joinKey = new String();
		String tableName = new String();
		
		public DoubleString(){}
		public DoubleString(String _joinKey, String _tableName)
		{
			joinKey = _joinKey;
			tableName = _tableName;
		}
		
		public void readFields(DataInput in) throws IOException
		{
			joinKey = in.readUTF();
			tableName = in.readUTF();
		}
		
		public void write(DataOutput out) throws IOException
		{
			out.writeUTF(joinKey);
			out.writeUTF(tableName);
		}
		
		public int compareTo(Object o1)
		{
			DoubleString o = (DoubleString) o1;
			
			int ret = joinKey.compareTo(o.joinKey);
			if(ret != 0) return ret;
			
			return tableName.compareTo(o.tableName);
		}
		
		public String toString() 
		{
			return joinKey + " " + tableName;
		}
	}
	
	public static class CompositeKeyComparator extends WritableComparator 
	{
		protected CompositeKeyComparator() 
		{
			super(DoubleString.class, true);
		}
		public int compare(WritableComparable w1, WritableComparable w2)
		{
			DoubleString k1 = (DoubleString) w1;
			DoubleString k2 = (DoubleString) w2;
			
			int result = k1.joinKey.compareTo(k2.joinKey);
			if(0 == result)
			{
				result = k1.tableName.compareTo(k2.tableName);
			}
			return result;
		}
	}	
	
	public static class FirstPartitioner extends Partitioner<DoubleString, Text>
	{
		public int getPartition(DoubleString key, Text value, int numPartition)
		{
			return key.joinKey.hashCode() % numPartition;
		}
	}
	
	public static class FirstGroupingComparator extends WritableComparator 
	{
		protected FirstGroupingComparator()
		{
			super(DoubleString.class, true);
		}
		public int compare(WritableComparable w1, WritableComparable w2)
		{
			DoubleString k1 = (DoubleString) w1;
			DoubleString k2 = (DoubleString) w2;
			
			return k1.joinKey.compareTo(k2.joinKey);
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length != 3) 
		{
			System.err.println("Usage: IMDB <in> <out> <k>");
			System.exit(2);
		}
		
		int topK = Integer.parseInt(otherArgs[2]);
		conf.setInt("topK", topK);
		
		Job job = new Job(conf, "IMDB");
		job.setJarByClass(IMDBStudent20191025.class);
		job.setMapperClass(IMDBMapper.class);
		job.setReducerClass(IMDBReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setMapOutputKeyClass(DoubleString.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(FirstGroupingComparator.class);
		job.setSortComparatorClass(CompositeKeyComparator.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
