package hadoop.join;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import hadoop.join.BroadcastJoin.BroadCastJoinMapper;
import hadoop.join.ImprovedRepartitionJoin.ImprovedRepartitionJoinMapper;
import hadoop.join.ImprovedRepartitionJoin.ImprovedRepartitionJoinReducer;
import hadoop.join.SemiJoin.SemiJoinMapper;
import hadoop.join.SemiJoin.SemiJoinReducer;

/**
 * ������Ȼ���ӣ�����cost model�Զ�ѡ����ʵ��㷨
 */
public class Join
{
	// Buffer��С
	private static final long nBlockMemory = 50;
	// ÿ��block�Ĵ�С(byte)
	private static final long blockSize = 64;

	private static final String semiJoinKeysFilename = "SemiJoinKeys.txt";

	// join����
	private static JoinMethod method;

	private enum JoinMethod
	{
		REPARTITION, BROADCAST, SEMI;
	}

	public static void main(String[] arg) throws Exception
	{
		Path inputPath = new Path("input/input8/");
		Path outputPath = new Path("output/output8/");
		// ȷ��join����
		method = JoinMethod.REPARTITION;
		File file = new File(inputPath.toString() + "/2.txt");
		long fileSize = file.length(); // ���ļ���С���ƹ��ƶ������������ܷ񱻶����ڴ�
		if (fileSize <= (nBlockMemory - 2) * blockSize) method = JoinMethod.BROADCAST;
		else
		{
			long lineNumber = 0;
			BufferedReader in = new BufferedReader(new FileReader(file));
			while (in.readLine() != null)
				lineNumber++;
			in.close();
			if (lineNumber * 4 <= (nBlockMemory - 2) * blockSize) method = JoinMethod.SEMI; // keyΪint���ͣ���������*4��Ϊkeys�б�Ĵ�С
		}
		System.out.println(method);
		// ִ��join
		Configuration conf = new Configuration();
		FileSystem.getLocal(conf).delete(outputPath, true);
		Job job = null;
		if (method == JoinMethod.REPARTITION)
		{
			job = new Job(conf);
			job.setJarByClass(ImprovedRepartitionJoin.class);
			job.setMapperClass(ImprovedRepartitionJoinMapper.class);
			job.setReducerClass(ImprovedRepartitionJoinReducer.class);
			job.setMapOutputKeyClass(ImprovedRepartitionJoin.TaggedRecord.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
		}
		else if (method == JoinMethod.SEMI)
		{
			SemiJoin.setNumberTable(2);
			SemiJoin.writeKeys(inputPath + "/2.txt");
			DistributedCache.addCacheFile(new Path(semiJoinKeysFilename).toUri(), conf);
			job = new Job(conf);
			job.setJarByClass(SemiJoin.class);
			job.setMapperClass(SemiJoinMapper.class);
			job.setReducerClass(SemiJoinReducer.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(SemiJoin.TaggedRecord.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);
		}
		else if (method == JoinMethod.BROADCAST)
		{
			if (new File(inputPath + "/files").mkdir()) new File(inputPath.toString() + "/1.txt").renameTo(new File(inputPath.toString() + "/files/1.txt"));
			DistributedCache.addCacheFile(new Path(inputPath + "/2.txt").toUri(), conf);
			inputPath = new Path(inputPath.toString() + "/files/");
			job = new Job(conf);
			job.setJarByClass(BroadcastJoin.class);
			job.setMapperClass(BroadCastJoinMapper.class);
			job.setNumReduceTasks(0);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);
		}
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
