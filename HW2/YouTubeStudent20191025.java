import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class YouTubeStudent20191025
{

	public static class Youtube 
	{
		public String category;
		public double average;
		
		public Youtube(String _category, double _average)
		{
			this.category = _category;
			this.average = _average;
		}
		public String getCategory() 
		{
			return this.category;
		}
		public double getAverage() 
		{
			return this.average;
		}
			
		public String getString()
		{
			return category + "|" + average;
		}
		
	}
	
	public static class YoutubeComparator implements Comparator<Youtube>
	{
		public int compare(Youtube x, Youtube y)
		{
			if (x.average > y.average) return 1;
			if (x.average < y.average) return -1;
			return 0;
		} 
	}
	
	public static void insertYoutube(PriorityQueue q, String category, double average, int topK)
	{
		Youtube youtube_head = (Youtube)q.peek();
		if(q.size() < topK || youtube_head.average < average)
		{
			Youtube youtube = new Youtube(category, average);
			q.add(youtube);
			if(q.size() > topK) q.remove();
		}
	}

	public static class YouTubeMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>
	{
		private PriorityQueue<Youtube> queue;
		private Comparator<Youtube> comp = new YoutubeComparator();
		private int topK;
		
		private Text map_key = new Text();
		private DoubleWritable map_val = new DoubleWritable();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			StringTokenizer itr = new StringTokenizer(value.toString(), "|");

			String id = itr.nextToken();
			String id2 = itr.nextToken();
			String id3 = itr.nextToken();
			String cate = itr.nextToken();
			String id4 = itr.nextToken();
			String id5 = itr.nextToken();
			double avr = Double.parseDouble(itr.nextToken());
			
			map_key.set(cate);
			map_val.set(avr);
			
			context.write(map_key, map_val);
		}
		
	}
	
	

	public static class YouTubeReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> 
	{
		private PriorityQueue<Youtube> queue;
		private Comparator<Youtube> comp = new YoutubeComparator();
		private int topK;
		
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context ) throws IOException, InterruptedException 
		{
		
			double sum = 0;
			int count = 0;
			for(DoubleWritable val: values)
			{
				sum += val.get();
				count++;
			}	
			
			double average = sum / count;
			
			insertYoutube(queue, key.toString(), average, topK);
		}
		protected void setup (Context context) throws IOException, InterruptedException
		{
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<Youtube> (topK, comp);
			
		}
		protected void cleanup (Context context) throws IOException, InterruptedException
		{
			
			List<Youtube> list = new ArrayList<Youtube>();
			while(queue.size() != 0 )
			{
				Youtube youtube = (Youtube) queue.remove();
				
				list.add(youtube);
				//context.write(new Text(youtube.getCategory()), new DoubleWritable( youtube.getAverage()));
			}
			for (int i=list.size()-1; i>=0; i--)
			{
				context.write(new Text(list.get(i).getCategory()), new DoubleWritable( list.get(i).getAverage()));
			}
			
		}
	}
	
	
	public static void main(String[] args) throws Exception 
	{
	
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		
		if(otherArgs.length != 3)
		{
			System.err.println("Usage: PageRank <in> <out> <k>");
			System.exit(2);
		}
		
		int topK = Integer.parseInt(otherArgs[2]);
		conf.setInt("topK", topK);
		
		Job job = new Job(conf, "YouTube");
			
		job.setJarByClass(YouTubeStudent20191025.class);
		job.setMapperClass(YouTubeMapper.class);
		job.setReducerClass(YouTubeReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
			
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	
	}
}
