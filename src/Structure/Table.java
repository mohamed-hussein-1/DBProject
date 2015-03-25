package Structure;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;

import team_7adota.DBAppException;

public class Table {
	
	private final static String page_name = "[tableName + \"-\" + pageNumber ]";
	String table_name;
	int pageSize; //number of pages
	String[] columns;
	Row[] tuples;
	
	ArrayList<Page> pages_loaded;
	ArrayList<Integer> pages_loaded_number;
	ArrayList<Boolean> pages_loaded_is_changed;
	
	
	public Table(String name){
		table_name = name;
		pageSize = 0;
		createNewPage();
		pages_loaded = new ArrayList<Page>();
		pages_loaded_number = new ArrayList<Integer>();
		pages_loaded_is_changed = new ArrayList<Boolean>();
	}
	
	// need to be implemented
	public static Table buildTableFromCSV(String filePath,String tableName){
		return new Table(tableName);
	}
	//insert row to the table :D
	public void insertRowToTable(Row row) throws DBAppException, PageNotLoadedException{
		//load the last page
		int indexInserted = loadPage(pageSize);
		//page is already Loaded handling
		if (indexInserted == -1)
			indexInserted = getPageIndexInArrayList(pageSize);
		
		//last page is full
		if (!pages_loaded.get(indexInserted).canInsertRow()){
			createNewPage();
			insertRowToTable(row);
		}
		
		addRowToPage(pageSize,row);
	}
	public void createNewPage(){
		int pp = pageSize + 1; 
		Page pageToInsert = new Page();
		pageToInsert.serializePage(this.table_name + "-" + pp + ".ser");
		++pageSize;
	}
	//not completed
	public int loadPage(int page_number) throws DBAppException{
		if (isPageLoaded(page_number))
			return -1;
		Page pageToLoad = null; // need to add here the code to write from file serialized
		if (page_number > pageSize){
			throw new DBAppException();
		}
		

		pageToLoad = Page.deSerializePage(this.table_name + "-" + page_number + ".ser");

		pages_loaded.add(pageToLoad);
		int indexInserted = pages_loaded.size()-1;
		pages_loaded_number.add(pages_loaded.size()-1);
		pages_loaded_is_changed.add(false);
		return indexInserted;
	}
	public boolean isPageLoaded(int page_number){
		int indexInMemory = getPageIndexInArrayList(page_number);
		if (indexInMemory == -1)
			return false;
		return true;
	}
	
	//this method returns the page (must be loaded) from the arraylist of pages
	public Page getPageByNumber(int page_number) throws DBAppException{
		loadPage(page_number);
		return pages_loaded.get(getPageIndexInArrayList(page_number));
	}
	public int getPageIndexInArrayList(int page_number){
		int pg = pages_loaded_number.size();
		for (int i = 0; i < pg; i++) {
			if (pages_loaded_number.get(i).intValue() == page_number){
				return i;
			}
		}
		return -1; // page not found
	}
	//this method removes the page from memory
	public void removePage(int page_number){
		int pageIndex = getPageIndexInArrayList(page_number);
		if (pageIndex == -1)
			return;
		pages_loaded.remove(pageIndex);
		pages_loaded_number.remove(pageIndex);
		pages_loaded_is_changed.remove(pageIndex);
		
	}
	//this method tells the memory that a specific page is overwritten and need to be saved
	public void recordChange(int page_number) throws PageNotLoadedException{
		int index = getPageIndexInArrayList(page_number); 
		if (index == -1)
			throw new PageNotLoadedException();
		Boolean newVal = pages_loaded_is_changed.get(index); 
		newVal= new Boolean(true);
	}
	
	// this method add a row to a page
	public void addRowToPage(int page_number,Row row) throws DBAppException, PageNotLoadedException{
		Page pageToInsert = getPageByNumber(page_number);
		
		pageToInsert.insertRow(row);
		
		recordChange(page_number);
	}
	//need to be implemented
	public void savePage(int page_number) throws DBAppException, IOException{
		int page_index = getPageIndexInArrayList(page_number);
		Page pageToInsert = this.getPageByNumber(page_number);
		OutputStream file = new FileOutputStream(this.table_name + "-" + page_number + ".ser");
		OutputStream buffer = new BufferedOutputStream(file);
		ObjectOutput output = new ObjectOutputStream(buffer);
		output.writeObject(pageToInsert);
		Boolean b = pages_loaded_is_changed.get(page_index);
		b = new Boolean("true");
	}
	public static void main(String[] args) throws DBAppException, PageNotLoadedException, IOException {
		Table t = new Table("t");
		Row row = new Row(1);
		String[] s = new String[1];
		s[0] = "first att";
		row.editColumnValue(0, s[0]);
		t.insertRowToTable(row);
		t.savePage(1);
		Row r = new Row(1);
		r.editColumnValue(0, "secondVal");
		t.insertRowToTable(r);
		t.savePage(1);
		t.loadPage(1);
		Page p = t.getPageByNumber(1);
		System.out.println(p.toString());
	}
}
