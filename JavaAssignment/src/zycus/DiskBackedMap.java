package zycus;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

/*
 * DiskBackedMap class is used to store large number of key-value pair in HashMap
 * Initially it will store all data in inMemoryHashMap.
 * Once maximum capacity of inMemoryHashMap is reached, it will store data in File System
 * Once maximum capacity of File system is reached it will throw DiskBackedMapCapacityException 
 */
public class DiskBackedMap 
{
	private HashMap<String, String> inMemoryHashMap;
	private HashMap<String, String> diskedHashMap;
	private int maxInMemoryHashMapCapacity;
	private String diskedMapFileFolder;
	private FileInputStream fileInputStream;
	private ObjectInputStream objectInputStream;
	private FileOutputStream fileOutputStream;
	private ObjectOutputStream objectOutputStream;
	private int maxNoOfDiskedMapFiles;
	private ArrayList<String> diskedMapFilePathList;
	
	
	/*
	 * First need to define how many key-value pairs can be stored in map which is there in memory
	 * maxInMemoryHashMapCapacity = Maximum limit to store key-value pair to map in Memory
	 * 
	 * Once maximum capacity is reached for InMemory map , data will be stored inside files
	 * maxNoOfDiskedMapFiles = How many files will be there to store data
	 * 
	 * diskedMapFileFolder = Folder where files with data will get stored
	 * 
	 */
	public DiskBackedMap(int maxInMemoryHashMapCapacity, int maxNoOfDiskedMapFiles , String diskedMapFileFolder) throws IOException
	{
		inMemoryHashMap = new HashMap<String,String>();
		diskedHashMap = new HashMap<String, String>();
		
		this.maxInMemoryHashMapCapacity = maxInMemoryHashMapCapacity ;
		this.maxNoOfDiskedMapFiles = maxNoOfDiskedMapFiles;
		this.diskedMapFileFolder = diskedMapFileFolder;
		
		diskedMapFilePathList = new ArrayList<String>();
		
		for(int i = 0 ; i < maxNoOfDiskedMapFiles ; i++)
		{
			diskedMapFilePathList.add(this.diskedMapFileFolder+"diskedMap_"+i+".txt");
		}
		
		writeAllDiskedHashMapToFileSystem();
	}
	
	
	/*
	 * This method will create all the empty files in file system.
	 * This created file will be used later to store data  
	 */
	private void writeAllDiskedHashMapToFileSystem() throws IOException
	{
		for(String diskedMapFilePath : diskedMapFilePathList)
		{
			writeDiskedHashMapToFileSystem(diskedMapFilePath);
		}
	}

	
	/*
	 * This method will create an empty file in file system for given path.
	 */
	private void writeDiskedHashMapToFileSystem(String diskedMapFilePath) throws IOException 
	{
		try
		{
			fileOutputStream = new FileOutputStream(diskedMapFilePath);
			objectOutputStream = new ObjectOutputStream(fileOutputStream);
			objectOutputStream.writeObject(diskedHashMap);
			
			fileOutputStream.flush();
		}
		catch(IOException ioException)
		{
			System.out.println("IOException in writeDiskedHashMapToFileSystem");
			ioException.printStackTrace();
		}
		finally
		{
			if(objectOutputStream != null)
			{
				objectOutputStream.close();
				objectOutputStream = null;
			}
			if(fileOutputStream != null)
			{
				fileOutputStream.close();
				fileOutputStream = null;
			}
		}
	}
	
	
	/*
	 * This method will return HashMap stored in that perticular file 
	 */
	private HashMap<String, String> getDiskHashMapFromFileSystem(String diskedMapFilePath) throws ClassNotFoundException, IOException
	{
		try
		{
			fileInputStream = new FileInputStream(diskedMapFilePath);
			objectInputStream = new ObjectInputStream(fileInputStream);
			diskedHashMap = (HashMap<String, String>)objectInputStream.readObject();
		}
		catch(IOException ioException)
		{
			System.out.println("IOException in getDiskHashMapFromFileSystem");
			ioException.printStackTrace();
		}
		finally
		{
			if(objectInputStream != null)
			{
				objectInputStream.close();
				objectInputStream = null;
			}
			if(fileInputStream != null)
			{
				fileInputStream.close();
				fileInputStream = null;
			}
		}
		return diskedHashMap;
	}
	
	
	/*
	 * This method will return true if hashMap is full
	 */
	private boolean isHashMapFull(HashMap<String, String> hashMap) 
	{
		if(hashMap.size() == maxInMemoryHashMapCapacity)
		{
			return true;
		}
		else
		{
			return false;
		}
	}

	
	/*
	 * Once maximum capacity is reached for InMemory map , 
	 * This method is used to find index of file where key-value pair will get stored 
	 */
	private int indexFor(int hashCode) 
	{
		return hashCode & (maxNoOfDiskedMapFiles - 1);
	}
	
	
	/*
	 * It will return available next index 
	 */
	private int getNextFileIndex(int fileIndex)
	{
		++fileIndex;
		if(fileIndex == maxNoOfDiskedMapFiles)
		{
			fileIndex = 0; 
		}
		return fileIndex;
	}
	
	
	/*
	 * if inMemoryHashMap is not full then it will put in inMemoryHashMap
	 * Once maximum capacity is reached for InMemory map , it will store key-value pair in File System.
	 * 
	 * For thread safety method is declared as synchronized
	 */
	public synchronized String put(String key,String value) throws ClassNotFoundException, IOException, DiskBackedMapCapacityException 
	{
		if(!isHashMapFull(inMemoryHashMap))
		{
			return inMemoryHashMap.put(key, value);
		}
		else
		{
			return putIntoDiskedHashMap(key,value);
		}
	}

	
	/*
	 * it will store key-value pair in File System.
	 * It will first identify index of file where key-value pair need to be store
	 * if file is full then it will try to add key-value pair in next index file
	 * if all the files are full then it will throw Exception DiskBackedMapCapacityException 
	 */
	private String putIntoDiskedHashMap(String key, String value) throws ClassNotFoundException, IOException, DiskBackedMapCapacityException 
	{
		String returnValue = "";
		
		int fileIndex = indexFor(key.hashCode());
		
		for(int i = 0 ; i < maxNoOfDiskedMapFiles ; i++)
		{
			diskedHashMap = getDiskHashMapFromFileSystem(diskedMapFilePathList.get(fileIndex));
			if(!isHashMapFull(diskedHashMap))
			{
				returnValue = diskedHashMap.put(key, value);
				writeDiskedHashMapToFileSystem(diskedMapFilePathList.get(fileIndex));
				return returnValue;
			}
			else
			{
				fileIndex = getNextFileIndex(fileIndex);
			}
		}
		//We have checked in all the Files in File System. But all the files are full.. Hence throwing Exception
		throw new DiskBackedMapCapacityException("All the file has reached its maximum capacity.. Memory error .. can not add '"+key+"' - '"+value+"'");
	}
	
	
	/*
	 * This method will return value for given key.
	 * if key is not found than it will return null.
	 * 
	 * If key is not found in inMemoryHashMap then it will search in File System for that key
	 * 
	 * For thread safety method is declared as synchronized
	 */
	public synchronized String get(String key) throws ClassNotFoundException, IOException 
	{
		if(inMemoryHashMap.containsKey(key))
		{
			return inMemoryHashMap.get(key);
		}
		else
		{
			return getFromDiskedHashMap(key);
		}
	}
	
	
	/*
	 * it will search for value in File System for given key
	 */
	private String getFromDiskedHashMap(String key) throws ClassNotFoundException, IOException 
	{
		String returnValue = "";
		
		int fileIndex = indexFor(key.hashCode());
		
		for(int i = 0 ; i < maxNoOfDiskedMapFiles ; i++)
		{
			diskedHashMap = getDiskHashMapFromFileSystem(diskedMapFilePathList.get(fileIndex));
			if(diskedHashMap.containsKey(key))
			{
				returnValue = diskedHashMap.get(key);
				return returnValue;
			}
			else
			{
				fileIndex = getNextFileIndex(fileIndex);
			}
		}
		
		//We have checked in all the Files in File System. But key is not found hence returning null..
		//System.out.println("Key '"+key+"' not found in all the files");
		return null;	
	}

	
	/*
	 * It will remove value from Map.
	 * If key is not found in inMemoryHashMap ,
	 * Then it will try to remove from file system.
	 * 
	 * For thread safety method is declared as synchronized
	 */
	public synchronized String remove(String key) throws ClassNotFoundException, IOException 
	{
		String removeValue = null;
		
		if(inMemoryHashMap.containsKey(key))
		{
			removeValue =  inMemoryHashMap.remove(key);
		}
		else
		{
			removeValue = removeFromDiskedHashMap(key);
		}
		return removeValue;
	}

	
	/*
	 * Removes key-value pair from File system
	 */
	private String removeFromDiskedHashMap(String key) throws ClassNotFoundException, IOException 
	{
		String returnValue = "";
		
		int fileIndex = indexFor(key.hashCode());
		
		for(int i = 0 ; i < maxNoOfDiskedMapFiles ; i++)
		{
			diskedHashMap = getDiskHashMapFromFileSystem(diskedMapFilePathList.get(fileIndex));
			if(diskedHashMap.containsKey(key))
			{
				returnValue = diskedHashMap.remove(key);
				writeDiskedHashMapToFileSystem(diskedMapFilePathList.get(fileIndex));
				return returnValue;
			}
			else
			{
				fileIndex = getNextFileIndex(fileIndex);
			}
		}
		
		//We have checked in all the Files in File System. But key is not found to remove.. hence returning null..
		//System.out.println("Key '"+key+"' not found in all the files");
		return null;
	}
	
	
	/*
	 * Printing entire DiskBackedMap
	 */
	public synchronized void printDiskBackedMap() throws ClassNotFoundException, IOException
	{
		printInMemoryHashMap();
		printDiskedHashMap();
	}
	
	
	/*
	 * printing inMemoryHashMap 
	 */
	private void printInMemoryHashMap()
	{
		System.out.println("printInMemoryHashMap start");
		//System.out.println("inMemoryHashMap.size() --> "+inMemoryHashMap.size());
		//System.out.println("maxInMemoryHashMapCapacity --> "+maxInMemoryHashMapCapacity);
		printHashMap(inMemoryHashMap);
		System.out.println("printInMemoryHashMap end");
	}
	
	
	/*
	 * printing values from file system
	 */
	private void printDiskedHashMap() throws ClassNotFoundException, IOException
	{
		System.out.println("printDiskedHashMap end");
		for(int i = 0 ; i < maxNoOfDiskedMapFiles ; i++)
		{
			//System.out.println("start of "+diskedMapFilePathList.get(i));
			printHashMap(getDiskHashMapFromFileSystem(diskedMapFilePathList.get(i)));
			//System.out.println("end of "+diskedMapFilePathList.get(i));
		}
		System.out.println("printDiskedHashMap end");
	}
	
	
	/*
	 * printing given map
	 */
	private void printHashMap(HashMap<String, String> hashMap)
	{
		Iterator iterator = hashMap.entrySet().iterator();
		
		while(iterator.hasNext())
		{
			Entry<String, String> entry = (Entry<String, String>) iterator.next();
			String key = entry.getKey();
			String value = entry.getValue();
			System.out.println(key + " - " + value);
		}
	}
	
	
}//end DiskBackedMap class