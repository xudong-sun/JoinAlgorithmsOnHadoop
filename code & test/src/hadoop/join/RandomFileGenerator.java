package hadoop.join;

import java.io.File;
import java.io.PrintWriter;
import java.util.Random;

/**
 * ������������ļ�
 */
public class RandomFileGenerator
{
	// �����ļ����ļ���
	private static String filename = "2.txt";
	// ������key�ķ�Χ��1-maxID֮���������
	private static int maxID = 10000;
	// ��������������
	private static int numRecord = 100;
	// �������ݵ�value��һ��String���ͣ�����
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
