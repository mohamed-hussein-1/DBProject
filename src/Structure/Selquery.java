package Structure;

public class Selquery {
	
	Row row;
	int page_number;
	int row_number;
	
	public Selquery(Row result,int page_number,int row_number){
		this.row = result;
		this.page_number = page_number;
		this.row_number = row_number;
	}
	 public Row getRow(){
		 return row;
	 }
	 public String toString(){
		 String s = "page_number = " + page_number;
		 s += " , row_number = " + row_number;
		 s += row.toString();
		 return s;
	 }
	
	
}
