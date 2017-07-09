package zycus;

import java.io.IOException;

public class DiskBackedMapMain 
{
	public static void main(String[] args) throws ClassNotFoundException, IOException 
	{
		DiskBackedMap diskBackedMap = null;
		
		System.out.println("--------------------------Adding value to DiskBackedMap start--------------------------");
		
		try
		{
			/*
			Assuming that at a time storage of 20 key-value pair is allowed inMemory
			(Worst case InMemoryMap will consist of 10 values and One of the Disk map consist of 10 values)
			Total 10 + 10 = 20 max values are allowed in memory
			Assuming there are 5 files to store data
			10 (In memory) + 5 * 10 (Each file can store upto 10 values) = 60
			Final Maximum capacity of DiskBackedMap s 60, After which it will throw Exception
			*/
			diskBackedMap = new DiskBackedMap(10,5,"E:/SagarIOLearning/");
			
			for(int i = 1 ; i < 70 ; i++)
			{
				diskBackedMap.put(String.valueOf(i), String.valueOf(i));
			}
		
		}
		catch(Exception exception)
		{
			exception.printStackTrace();
		}
		System.out.println("--------------------------Adding value to DiskBackedMap end--------------------------");
		
		System.out.println("--------------------------Printing DiskBackedMap start--------------------------");
			diskBackedMap.printDiskBackedMap();
		System.out.println("--------------------------Printing DiskBackedMap end--------------------------");
		
		
		System.out.println("--------------------------Getting value from DiskBackedMap start--------------------------");
		System.out.println("diskBackedMap.get('1') --> " +diskBackedMap.get("1"));
		System.out.println("diskBackedMap.get('21') -->"+diskBackedMap.get("21"));
		System.out.println("diskBackedMap.get('61') -->"+diskBackedMap.get("61"));
		System.out.println("--------------------------Getting value from DiskBackedMap end--------------------------");
		
		System.out.println("--------------------------Removing and Getting value from DiskBackedMap start--------------------------");
		System.out.println("diskBackedMap.remove('1') --> " +diskBackedMap.remove("1"));
		System.out.println("diskBackedMap.remove('21') -->"+diskBackedMap.remove("21"));
		System.out.println("diskBackedMap.remove('61') -->"+diskBackedMap.remove("61"));
		System.out.println("diskBackedMap.get('1') --> " +diskBackedMap.get("1"));
		System.out.println("diskBackedMap.get('21') -->"+diskBackedMap.get("21"));
		System.out.println("diskBackedMap.get('61') -->"+diskBackedMap.get("61"));
		System.out.println("--------------------------Removing and Getting value from DiskBackedMap end--------------------------");
		
		System.out.println("--------------------------Printing DiskBackedMap start--------------------------");
		diskBackedMap.printDiskBackedMap();
		System.out.println("--------------------------Printing DiskBackedMap end--------------------------");
	}
}
