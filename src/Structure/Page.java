package Structure;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;

import team_7adota.DBAppException;

public class Page implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -9203234827984854096L;
	int tuple_size; //number of tuples inside the page
	final int max_rows = 20; //max number of tuples inside the page
	Row[] tuples;
	int column_number;
	
	public Page(){
		tuples = new Row[max_rows];
		tuple_size = 0;
		column_number = 0;
	}
	
	public void insertRow(Row rowToInsert) throws MaxRowsException{
		if (tuple_size == max_rows){
			throw new MaxRowsException();
		}
			
		tuples[tuple_size] = rowToInsert;
		++tuple_size;
	}
	
	public void deleteRow(int index) throws DBAppException{
		if (index >= tuple_size)
			throw new DBAppException(); // ROw not found
		tuples[index].deleteRow();
	}
	public boolean canInsertRow(){
		if (max_rows == tuple_size)
			return false;
		return true;
	}
	public String toString(){
		String s = "";
		for (int i = 0; i < this.tuple_size; i++) {
			s = s + this.tuples[i].toString();
			s += "\n";
		}
		return s;
	}
	public void serializePage(String path){
		try (
				OutputStream file = new FileOutputStream(path);
//				OutputStream buffer = new BufferedOutputStream(file);
				ObjectOutput output = new ObjectOutputStream(file);
				){
			output.writeObject(this);
			output.flush();
		}  
		catch(IOException ex){
			ex.printStackTrace();
		}
	}
	public static Page deSerializePage(String path){
		try(
			      InputStream file = new FileInputStream(path);
			      InputStream buffer = new BufferedInputStream(file);
			      ObjectInput input = new ObjectInputStream (buffer);
			    ){
			      //deserialize the List
			       return (Page)input.readObject();
			      //display its data
			      
			    }
			    catch(ClassNotFoundException ex){
			      ex.printStackTrace();
			    }
			    catch(IOException ex){
			      ex.printStackTrace();
			    }
		return null;
	}
	public static void main(String[] args) throws MaxRowsException {
//		Page p = new Page();
//		p.serializePage("p.ser");
//		Page p = Page.deSerializePage("p.ser");
//		Row r = new Row(1);
//		r.editColumnValue(0,"tttt");
//		p.insertRow(r);
//		p.serializePage("p.ser");
		Page q = Page.deSerializePage("p.ser");
		System.out.println(q);
	}
	public static void test1() throws MaxRowsException{
		Page p = new Page();
		p.column_number = 1;
		Row r = new Row(2);
		r.editColumnValue(0, "mrSehs");
		r.editColumnValue(1, "mrAly");
		p.insertRow(r);
		Row x = new Row(2);
		x.editColumnValue(0, "mrYassin");
		x.editColumnValue(1, "mrTat");
		p.insertRow(x);
		p.serializePage("testpage.ser");
		Page q = deSerializePage("testpage.ser");
		System.out.println(q.toString());
	}
	public static void test2(){
		Page q;
		q = Page.deSerializePage("n-1.ser");
		System.out.println(q.toString());
		
	}
	
	
}
