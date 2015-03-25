package team_7adota;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class DBApp {

	final File metadatafile = new File("/home/bassem/Desktop/Eclipse Workspace/DB7adota/data/metadata.csv");
	final String metadataheader = "Table Name, Column Name, Column Type, Key, Indexed, References";
	final String comma = ",";
	final String newline = "\n";
	String temp;
	
	public DBApp()
	{
		init();	
	}
	public void init( )
	{
		FileWriter metadata = null;
		try {
			metadata = new FileWriter(metadatafile);
			
			metadata.append(metadataheader);
			metadata.append(newline);
			
			System.out.println("Metadata file was created successfully!");
			} catch (Exception e) {
			System.out.println("Error in CsvFileWriter !!!");
			e.printStackTrace();
			} finally {
			try {
			metadata.flush();
			metadata.close();
			} catch (IOException e) {
			System.out.println("Error while flushing/closing fileWriter !!!");
			e.printStackTrace();}}
	
	}
	public void createTable(String strTableName,Hashtable<String,String> htblColNameType,
	Hashtable<String,String>htblColNameRefs,String strKeyColName) throws DBAppException, IOException
	{
		// From CSV to arraylist<string[]>
		BufferedReader br = null;
		String line = "";
		ArrayList<String[]> temp = new ArrayList<String[]>();
		try {
			br = new BufferedReader(new FileReader(metadatafile));
			while ((line = br.readLine()) != null) 
			{
				String[] templine = line.split(comma);
				temp.add(templine);
			}
		} 
		catch (FileNotFoundException e)
		{e.printStackTrace();} 
		catch (IOException e) 
		{e.printStackTrace();} 
		finally {
		if (br != null) {
		try {br.close();} 
		catch (IOException e) {e.printStackTrace();}
		}}
		 
		FileWriter metadata = null;
		try {
			
			metadata = new FileWriter(metadatafile);
			// Adding old metadata file
			for (int i = 0; i < temp.size(); i++) 
			{
				for (int j = 0; j < temp.get(i).length; j++) 
				{
					metadata.append(temp.get(i)[j]);
					metadata.append(comma);
				}
				metadata.append(newline);
			}
			
			// Adding table schema to metadata
			String str;
			Set<String> set = htblColNameType.keySet();
			Iterator<String> itr = set.iterator();
			while (itr.hasNext()) 
			{	
				str = itr.next();
				// Table Name
				metadata.append(strTableName);
				metadata.append(comma);
				// Col Name
				metadata.append(str);  
				metadata.append(comma);
				// Col type
				metadata.append(htblColNameType.get(str));
				metadata.append(comma);
				// boolean key
				if(str == strKeyColName)
				{metadata.append("true");}
				else
				{metadata.append("false");}
				metadata.append(comma);
				// boolean indexed
				if(str == strKeyColName)
				{metadata.append("true");}
				else
				{metadata.append("false");}
				metadata.append(comma);
				// boolean referenced
				if(htblColNameRefs.containsKey(str))
				{metadata.append(htblColNameRefs.get(str));}
				else
				{metadata.append("null");}
				metadata.append(comma);
				// New Line
				metadata.append(newline);
			}
		
		System.out.println("Table was added successfully!");
		} catch (Exception e) {
		System.out.println("Error in CsvFileWriter !!!");
		e.printStackTrace();
		} finally {
		try {
		metadata.flush();
		metadata.close();
		} catch (IOException e) {
		e.printStackTrace();}}
		
		// Create Page to add tuples to it
		FileOutputStream x = new FileOutputStream("/home/bassem/Desktop/Eclipse Workspace/DB7adota/data/" + strTableName +".ser");
		x.close();
	}
	public void createIndex(String strTableName,String strColName) throws DBAppException
	{
		// From CSV to arraylist<string[]>
			BufferedReader br = null;
			String line = "";
			ArrayList<String[]> temp = new ArrayList<String[]>();
			try {
				br = new BufferedReader(new FileReader(metadatafile));
				while ((line = br.readLine()) != null) 
				{
					String[] templine = line.split(comma);
					temp.add(templine);
				}
			} 
			catch (FileNotFoundException e)
			{e.printStackTrace();} 
			catch (IOException e) 
			{e.printStackTrace();} 
			finally {
			if (br != null) {
			try {br.close();} 
			catch (IOException e) {e.printStackTrace();}
			}}
				
			// Editing metadata file
			for (int i = 0; i < temp.size(); i++) 
			{
				if(temp.get(i)[0] == strTableName && temp.get(i)[1] == strColName)
				{
					temp.get(i)[4].equals("true");
				}
			}
				 
			FileWriter metadata = null;
			try {
					
				metadata = new FileWriter(metadatafile);
					
				// Adding old metadata file
				for (int i = 0; i < temp.size(); i++) 
				{
					for (int j = 0; j < temp.get(i).length; j++) 
					{
						metadata.append(temp.get(i)[j]);
						metadata.append(comma);
					}
					metadata.append(newline);
				}
				System.out.println(strColName + " is now index!");
				} 
			catch (Exception e) {
			System.out.println("Error in CsvFileWriter !!!");
			e.printStackTrace();
			} finally {
			try {
			metadata.flush();
			metadata.close();
			} catch (IOException e) {
			e.printStackTrace();}}
	}
	public void insertIntoTable(String strTableName,Hashtable<String,String> htblColNameValue)throws DBAppException
	{
		String str;
		Set<String> set = htblColNameValue.keySet();
		Iterator<String> itr = set.iterator();
		while (itr.hasNext()) 
		{
			
		}
		
	}
	
	public static void main(String[] args) throws DBAppException, IOException {
		
		DBApp x = new DBApp();
		Hashtable<String,String> ht = new Hashtable<String,String>();
		ht.put("name","java.lang.String");
		ht.put("id","java.lang.Integer");
		ht.put("age","java.lang.Integer");
		Hashtable<String,String> htr = new Hashtable<String,String>();
		htr.put("age","person.age");
		x.createTable("7adotatest",ht,htr,"id");
		//x.createIndex("7adotatest", "age");
		
	}
}