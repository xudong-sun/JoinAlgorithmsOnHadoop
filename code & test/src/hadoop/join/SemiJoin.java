package hadoop.join;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 多表自然连接semi join算法
 */
public class SemiJoin
{
	// 参与自然连接的表的数量
	private static int numberTable = 3;
	// 临时存放的keys集合文件
	private static final String keysFilename = "SemiJoinKeys.txt";

	static void setNumberTable(int n)
	{
		numberTable = n;
	}

	// 将小表的keys储存在一个单独的文件中，以便broadcast到所有节点
	static void writeKeys(String filename) throws Exception
	{
		BufferedReader in = null;
		PrintWriter out = null;
		try
		{
			in = new BufferedReader(new FileReader(new File(filename)));
			out = new PrintWriter(keysFilename);
			String ss = "";
			while ((ss = in.readLine()) != null)
			{
				out.println(ss.substring(0, ss.indexOf("\t")));
			}
		}
		finally
		{
			in.close();
			out.close();
		}
	}

	/**
	 * 带有tag的value。tag记录数据的来源(哪个表)
	 */
	static class TaggedRecord implements Writable
	{
		public TaggedRecord()
		{
		}

		public TaggedRecord(String value, byte tag)
		{
			this.value = value;
			this.tag = tag;
		}

		public String value;
		public byte tag;

		@Override
		public void readFields(DataInput in) throws IOException
		{
			tag = in.readByte();
			value = in.readUTF();
		}

		@Override
		public void write(DataOutput out) throws IOException
		{
			out.writeByte(tag);
			out.writeUTF(value);
		}
	}

	/**
	 * Mapper
	 * <p>
	 * setup阶段：读入被broadcast的keys列表，存放在一个HashSet中<br>
	 * map阶段：只保留key属于集合keys中的数据
	 */
	static class SemiJoinMapper extends Mapper<Object, Text, IntWritable, TaggedRecord>
	{
		private static HashSet<Integer> keys = new HashSet<Integer>();

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
						keys.add(Integer.parseInt(ss));
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
			InputSplit inputSplit = context.getInputSplit();
			String fileName = ((FileSplit) inputSplit).getPath().getName().toString();
			byte tag = (byte) Byte.parseByte(fileName.substring(0, fileName.lastIndexOf(".")));
			String s = value.toString();
			int tab = s.indexOf("\t");
			int id = Integer.parseInt(s.substring(0, tab));
			String ss = s.substring(tab + 1);
			if (keys.contains(id)) context.write(new IntWritable(id), new TaggedRecord(ss, tag));
		}
	}

	/**
	 * 和RepartitionJoin中的Reducer相同
	 */
	static class SemiJoinReducer extends Reducer<IntWritable, TaggedRecord, IntWritable, Text>
	{
		private ArrayList<ArrayList<String>> records = new ArrayList<ArrayList<String>>();

		@Override
		public void reduce(IntWritable key, Iterable<TaggedRecord> values, Context context) throws IOException, InterruptedException
		{
			records.clear();
			for (int i = 0; i < numberTable; i++)
				records.add(new ArrayList<String>());
			for (TaggedRecord record : values)
			{
				records.get(record.tag - 1).add(record.value);
			}
			boolean canJoin = true;
			for (int i = 0; i < numberTable; i++)
				if (records.get(i).size() == 0)
				{
					canJoin = false;
					break;
				}
			if (canJoin) iterateRecords(0, 0, new String[numberTable], key, context);
		}

		private void iterateRecords(int i, int currentIndex, String[] currentRecords, IntWritable key, Context context) throws IOException, InterruptedException
		{
			if (i == numberTable)
			{
				StringBuilder string = new StringBuilder(currentRecords[0]);
				for (int j = 1; j < numberTable; j++)
				{
					string.append("\t");
					string.append(currentRecords[j]);
				}
				context.write(key, new Text(string.toString()));
				return;
			}
			currentRecords[i] = records.get(i).get(currentIndex);
			iterateRecords(i + 1, 0, currentRecords, key, context);
			if (currentIndex < records.get(i).size() - 1) iterateRecords(i, currentIndex + 1, currentRecords, key, context);
		}
	}

	public static void main(String[] args) throws Exception
	{
		writeKeys("input/input3/2.txt"); // 先记录小表中的keys
		Configuration conf = new Configuration();
		Path inputPath = new Path("input/input3");
		Path outputPath = new Path("output/output3");
		FileSystem.getLocal(conf).delete(outputPath, true);
		DistributedCache.addCacheFile(new Path(keysFilename).toUri(), conf); // broadcast
		Job job = new Job(conf);
		job.setJarByClass(SemiJoin.class);
		job.setMapperClass(SemiJoinMapper.class);
		job.setReducerClass(SemiJoinReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(TaggedRecord.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
