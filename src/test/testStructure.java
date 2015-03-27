package test;

import java.io.IOException;

import team_7adota.DBAppException;
import Structure.Page;
import Structure.PageNotLoadedException;
import Structure.Row;
import Structure.Table;


public class testStructure {
	
	public static void main(String[] args) throws DBAppException, PageNotLoadedException, IOException {
		testSelInCustomer();
	}
	
	//first test to create table with name customer
	public static void createCustomer() throws DBAppException, PageNotLoadedException, IOException{
		Table t = new Table("customer");
		
		String[] col = new String[4];
		col[0] = "id" ;
		col[1] = "first_name" ;
		col[2] = "last_name";
		col[3] = "dollars_spent";
		
		
		t.setColumns(col);
		
		for (int i = 0; i < 90; i++) {
			Row r = new Row(4);
			String id = Integer.toString(i);
			String first_name = "name" + id;
			String last_name = "Lname" + id;
			String dollars_spent = Integer.toString(i*10) ;
			
			String[] sss = new String[4];
			sss[0] = id;
			sss[1] = first_name;
			sss[2] = last_name;
			sss[3] = dollars_spent;
			
			r.insertDataIntoRow(sss);
			t.insertRowToTable(r);
		}
		t.save();
	}
	public static void LoadCustomer() throws DBAppException{
		Table t = Table.buildBuildedTable("customer", 5);
		Page[] pp = new Page[5];
		for (int i = 1; i <= 5; i++) {
			t.loadPage(i);
			try {
				pp[i-1] = t.getPageByNumber(i);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println(pp[i-1]);
			System.out.println("end of page " + i);
		}
	}
	public static void LaunchTestCustomer() throws DBAppException, PageNotLoadedException, IOException{
		createCustomer();
		LoadCustomer();
	}
	
	public static void testSelInCustomer() throws DBAppException{
		Table t = Table.buildBuildedTable("customer",5);
		String[] col = new String[4];
		col[0] = "id" ;
		col[1] = "first_name" ;
		col[2] = "last_name";
		col[3] = "dollars_spent";
		t.setColumns(col);
		
		System.out.println(t.selectLinearyValueFromTable("id","5"));
	}
	
}
