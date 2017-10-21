package hadoop.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
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
 * 多表自然连接repartition join算法
 */
public class RepartitionJoin
{
	private static int numberTable = 3;

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
	 * Mapper: 获取key和value，并给value绑上tag
	 */
	static class RepartitionJoinMapper extends Mapper<Object, Text, IntWritable, TaggedRecord>
	{
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
			context.write(new IntWritable(id), new TaggedRecord(ss, tag));
		}
	}

	/**
	 * Reducer: 对于每个key，记录每个tag下的value数组。若所有的数组都非空，则输出所有组合
	 */
	static class RepartitionJoinReducer extends Reducer<IntWritable, TaggedRecord, IntWritable, Text>
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

		// 递归输出所有组合
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
		Configuration conf = new Configuration();
		Path inputPath = new Path("input\\input1\\");
		Path outputPath = new Path("output\\output1\\");
		FileSystem.getLocal(conf).delete(outputPath, true);
		Job job = new Job(conf);
		job.setJarByClass(RepartitionJoin.class);
		job.setMapperClass(RepartitionJoinMapper.class);
		job.setReducerClass(RepartitionJoinReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(TaggedRecord.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
