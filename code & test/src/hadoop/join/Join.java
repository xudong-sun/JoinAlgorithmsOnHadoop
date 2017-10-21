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
 * 两表自然连接，基于cost model自动选择合适的算法
 */
public class Join
{
	// Buffer大小
	private static final long nBlockMemory = 50;
	// 每个block的大小(byte)
	private static final long blockSize = 64;

	private static final String semiJoinKeysFilename = "SemiJoinKeys.txt";

	// join方法
	private static JoinMethod method;

	private enum JoinMethod
	{
		REPARTITION, BROADCAST, SEMI;
	}

	public static void main(String[] arg) throws Exception
	{
		Path inputPath = new Path("input/input8/");
		Path outputPath = new Path("output/output8/");
		// 确定join方法
		method = JoinMethod.REPARTITION;
		File file = new File(inputPath.toString() + "/2.txt");
		long fileSize = file.length(); // 用文件大小近似估计读进来的数据能否被读入内存
		if (fileSize <= (nBlockMemory - 2) * blockSize) method = JoinMethod.BROADCAST;
		else
		{
			long lineNumber = 0;
			BufferedReader in = new BufferedReader(new FileReader(file));
			while (in.readLine() != null)
				lineNumber++;
			in.close();
			if (lineNumber * 4 <= (nBlockMemory - 2) * blockSize) method = JoinMethod.SEMI; // key为int类型，数据行数*4即为keys列表的大小
		}
		System.out.println(method);
		// 执行join
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
