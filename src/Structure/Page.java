package Structure;

import java.io.BufferedInputStream;
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
import java.util.ArrayList;

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
	public ArrayList<Selquery> selAllFromPage(int page_number){
		ArrayList<Selquery> result = new ArrayList<Selquery>();
		for (int i = 0; i < this.tuple_size; i++) {
			Selquery q = new Selquery(tuples[i],page_number,i);
			result.add(q);
		}
		return result;
	}
}
