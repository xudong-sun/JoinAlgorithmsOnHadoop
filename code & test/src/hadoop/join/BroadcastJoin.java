package hadoop.join;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 两表自然连接broadcat join算法
 */
public class BroadcastJoin
{

	/**
	 * Mapper
	 * <p>
	 * setup阶段：读入被roadcast的文件，储存为HashMap<br>
	 * map阶段：直接join
	 */
	static class BroadCastJoinMapper extends Mapper<Object, Text, IntWritable, Text>
	{
		private static HashMap<Integer, LinkedList<String>> broadcast = new HashMap<Integer, LinkedList<String>>();

		@Override
		public void setup(Context context) throws IOException, InterruptedException
		{
			BufferedReader in = null;
			URI[] uris = DistributedCache.getCacheFiles(context.getConfiguration());
			for (URI uri : uris)
			{
				try
				{
					in = new BufferedReader(new FileReader(uri.toString()));
					String ss = "";
					while ((ss = in.readLine()) != null)
					{
						int tab = ss.indexOf("\t");
						int id = Integer.parseInt(ss.substring(0, tab));
						String record = ss.substring(tab + 1);
						if (broadcast.containsKey(id)) broadcast.get(id).add(record);
						else broadcast.put(id, new LinkedList<String>(Arrays.asList(record)));
					}
				}
				finally
				{
					in.close();
				}
			}
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String s = value.toString();
			int tab = s.indexOf("\t");
			int id = Integer.parseInt(s.substring(0, tab));
			String ss = s.substring(tab + 1);
			if (broadcast.containsKey(id))
			{
				for (String record : broadcast.get(id))
					context.write(new IntWritable(id), new Text(ss + "\t" + record));
			}
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		Path inputPath = new Path("input/input2/broadcast");
		Path outputPath = new Path("output/output2");
		FileSystem.getLocal(conf).delete(outputPath, true);
		DistributedCache.addCacheFile(new Path("input/input2/2.txt").toUri(), conf); // broadcast
		Job job = new Job(conf);
		job.setJarByClass(BroadcastJoin.class);
		job.setMapperClass(BroadCastJoinMapper.class);
		job.setNumReduceTasks(0); // map-only
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
