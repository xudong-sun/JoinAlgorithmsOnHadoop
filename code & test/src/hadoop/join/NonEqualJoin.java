package hadoop.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
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
 * ����ǵ�ֵ����
 */
public class NonEqualJoin
{
	/**
	 * ����tag��key��tag��¼���ݵ���Դ(�ĸ���)<br>
	 * compareTo��valueΪ��һ�ؼ��ʣ�tagΪ�ڶ��ؼ���
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

		public int value;
		public Byte tag;

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
			return this.value == record.value ? this.tag.compareTo(record.tag) : (this.value < record.value ? -1 : 1);
		}
	}

	/**
	 * Mapper: ��ȡkey��value������key����tag
	 */
	static class NonEqualJoinMapper extends Mapper<Object, Text, TaggedRecord, Text>
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
	 * Reducer��joinKeyС�Ļ��ȱ����룬����������������R�����¼��һ��LinkedList�У��������S��
	 * ������LinkedList������Ԫ��join�����
	 */
	static class NonEqualJoinReducer extends Reducer<TaggedRecord, Text, NullWritable, Text>
	{
		private LinkedList<String> records = new LinkedList<String>();

		@Override
		public void reduce(TaggedRecord key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			switch (key.tag)
			{
			case 1: // R��
				for (Text value : values)
					records.add(key.value + "\t" + value);
			case 2: // S��
				for (Text value : values)
					for (String string : records)
						context.write(null, new Text(string + "\t" + key.value + "\t" + value));
			}
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		Path inputPath = new Path("input\\input5\\");
		Path outputPath = new Path("output\\output5\\");
		FileSystem.getLocal(conf).delete(outputPath, true);
		Job job = new Job(conf);
		job.setJarByClass(NonEqualJoin.class);
		job.setMapperClass(NonEqualJoinMapper.class);
		job.setReducerClass(NonEqualJoinReducer.class);
		job.setMapOutputKeyClass(TaggedRecord.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
