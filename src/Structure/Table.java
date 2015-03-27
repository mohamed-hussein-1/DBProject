package Structure;

import java.io.IOException;
import java.util.ArrayList;

import team_7adota.DBAppException;

public class Table {
	
	@SuppressWarnings("unused")
	private final static String page_name = "[tableName + \"-\" + pageNumber ]";
	
	String table_name;
	int pageSize; //number of pages
	String[] columns;
	
	Row[] tuples;
	
	ArrayList<Page> pages_loaded;
	ArrayList<Integer> pages_loaded_number;
	ArrayList<Boolean> pages_loaded_is_changed;
	
	public Table(){
		
	}
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
	//just for tests
	public static Table buildBuildedTable(String tableName,int pageSize){
		Table tt = new Table();
		tt.table_name = tableName;
		tt.pageSize = pageSize;
		tt.pages_loaded = new ArrayList<Page>();
		tt.pages_loaded_is_changed = new ArrayList<Boolean>();
		tt.pages_loaded_number = new ArrayList<Integer>();
		return tt;
	}
	
	public String[] getColumns() {
		return columns;
	}
	public void setColumns(String[] columns) {
		this.columns = columns;
	}
	public int getColumnIndex(String col) throws DBAppException{
		int colSize = columns.length;
		for (int i = 0;i<colSize;i++) {
			String s = columns[i];
			if (s.equals(col))
				return i;
		}
		
		//column not found
		throw new DBAppException();
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
		
		String path = this.table_name+"-"+page_number+".ser";
		pageToLoad = Page.deSerializePage(path);
		pages_loaded.add(pageToLoad);
		int indexInserted = pages_loaded.size()-1;
		pages_loaded_number.add(page_number);
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
//		Boolean newVal = pages_loaded_is_changed.get(index); 
//		newVal= new Boolean(true);
		pages_loaded_is_changed.set(index,new Boolean(true));
	}
	
	// this method add a row to a page
	public void addRowToPage(int page_number,Row row) throws DBAppException, PageNotLoadedException{
		Page pageToInsert = getPageByNumber(page_number);
		
		pageToInsert.insertRow(row);
		
		recordChange(page_number);
	}
	//this method serialize the page number ( save it on the disk )
	public void savePage(int page_number) throws DBAppException, IOException{
		int page_index = getPageIndexInArrayList(page_number);
		Page pageToInsert = this.getPageByNumber(page_number);
		if (pages_loaded_is_changed.get(page_index).booleanValue() == false){
			return;
		}
		pageToInsert.serializePage(this.table_name + "-" + page_number + ".ser");
//		Boolean b = pages_loaded_is_changed.get(page_index);
//		b = new Boolean(false);
		pages_loaded_is_changed.set(page_index,new Boolean(false));
	}
	// this method saves all loaded pages
	public void save() throws DBAppException, IOException{
		int numberOfPages = pages_loaded.size();
		for (int i = 1; i <= numberOfPages; i++) {
			savePage(i);
		}
	}
	
	public ArrayList<Selquery> selectLinearyValueFromTable(String column_name,String column_value) throws DBAppException{
		
		//load all pages
		for(int i = 0; i <this.pageSize ; i++){
			loadPage(i+1);
		}
		
		//get ColumnIndexInRows
		int colIndex = this.getColumnIndex(column_name);
		
		ArrayList<Selquery> result = new ArrayList<Selquery>();
		
		//iterate over rows , solution won't be sorted at all
		int numberOfPagesLoaded = pages_loaded.size();
		for (int i = 0; i < numberOfPagesLoaded; i++) {
			Page p = pages_loaded.get(i);
			int p_number = pages_loaded_number.get(i).intValue();
			ArrayList<Selquery> qu = p.selAllFromPage(p_number);
			int quSize = qu.size();
			for (int j = 0; j < quSize; j++) {
				Row r = qu.get(j).getRow();
				String v = r.getColumnValue(colIndex);
				if (v.equals(column_value)){
					result.add(qu.get(j));
				}
			}
		}
		return result;
		
	}
	
	
	
	public static void main(String[] args) throws DBAppException, PageNotLoadedException, IOException {
		
	}
	
}
