package hadoop.join;

import java.io.File;
import java.io.PrintWriter;
import java.util.Random;

/**
 * 产生随机测试文件
 */
public class RandomFileGenerator
{
	// 产生文件的文件名
	private static String filename = "2.txt";
	// 产生的key的范围（1-maxID之间的整数）
	private static int maxID = 10000;
	// 产生的数据行数
	private static int numRecord = 100;
	// 产生数据的value（一个String类型）长度
	private static int lengthString = 10;

	private static Random random = new Random();

	public static void main(String[] args) throws Exception
	{
		PrintWriter out = new PrintWriter(new File(filename));
		for (int i = 0; i < numRecord; i++)
		{
			out.print((random.nextInt(maxID) + 1) + "\t");
			for (int j = 0; j < lengthString; j++)
				out.print((char) (random.nextInt(26) + 'a'));
			out.println();
		}
		out.close();
	}

}
