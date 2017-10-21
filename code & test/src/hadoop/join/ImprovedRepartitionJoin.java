package hadoop.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * ������Ȼ����improved repartition join�㷨
 */
public class ImprovedRepartitionJoin
{
	/**
	 * ����tag��key��tag��¼���ݵ���Դ(�ĸ���)<br>
	 * compareTo��tagΪ��һ�ؼ��ʣ�valueΪ�ڶ��ؼ���
	 */
	static class TaggedRecord implements WritableComparable<TaggedRecord>
	{
		public TaggedRecord()
		{
		}

		public TaggedRecord(int value, byte tag)
		{
			this.value = value;
			this.tag = tag;
		}

		public Integer value;
		public byte tag;

		@Override
		public void readFields(DataInput in) throws IOException
		{
			tag = in.readByte();
			value = in.readInt();
		}

		@Override
		public void write(DataOutput out) throws IOException
		{
			out.writeByte(tag);
			out.writeInt(value);
		}

		@Override
		public int compareTo(TaggedRecord record)
		{
			return this.tag == record.tag ? this.value.compareTo(record.value) : (this.tag < record.tag ? -1 : 1);
		}
	}

	/**
	 * Mapper: ��ȡkey��value������key����tag
	 */
	static class ImprovedRepartitionJoinMapper extends Mapper<Object, Text, TaggedRecord, Text>
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
			context.write(new TaggedRecord(id, tag), new Text(ss));
		}
	}

	/**
	 * Reducer����values���ȶ������Ķ�������R������ݣ�ȫ�������ڴ��γ�һ��HashMap�������һ��������S������ݣ�
	 * ��HashMap��key�Ƚϣ����У��������Ӧ�����ӽ��
	 */
	static class ImprovedRepartitionJoinReducer extends Reducer<TaggedRecord, Text, NullWritable, Text>
	{
		private HashMap<Integer, LinkedList<String>> records = new HashMap<Integer, LinkedList<String>>();

		@Override
		public void reduce(TaggedRecord key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			if (key.tag == 1) // ��һ�׶Σ�����R������
			{
				if (!(records.containsKey(key.value))) records.put(key.value, new LinkedList<String>());
				for (Text value : values)
					records.get(key.value).add(value.toString());
			}
			else // �ڶ��׶Σ�����S������
			{
				assert key.tag == 2;
				for (Text value : values)
				{
					if (records.containsKey(key.value))
					{
						for (String string : records.get(key.value))
							context.write(null, new Text(key.value + "\t" + string + "\t" + value));
					}
				}
			}
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		Path inputPath = new Path("input\\input4\\");
		Path outputPath = new Path("output\\output4\\");
		FileSystem.getLocal(conf).delete(outputPath, true);
		Job job = new Job(conf);
		job.setJarByClass(ImprovedRepartitionJoin.class);
		job.setMapperClass(ImprovedRepartitionJoinMapper.class);
		job.setReducerClass(ImprovedRepartitionJoinReducer.class);
		job.setMapOutputKeyClass(TaggedRecord.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
