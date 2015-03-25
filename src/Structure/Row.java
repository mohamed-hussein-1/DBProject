package Structure;

import java.io.Serializable;
import java.util.Arrays;

public class Row implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -4072469858862925697L;
	private String[] values;
	private boolean tombStone;
	
	// Constructors
	
	public Row(int numberOfColumns){
		values = new String[numberOfColumns];
		tombStone = false;
	}
	
	public Row(int numberOfColumns,String[] column_values){
		values = new String[numberOfColumns];
		insertDataIntoRow(column_values);
		tombStone = false;
	}
	
	public Row(String[] column_values){
		insertDataIntoRow(column_values);
		tombStone = false;
	}
	
	// end of constructors
	
	// public Methods
	public void insertDataIntoRow(String[] column_values){ 
		values = column_values;
	}
	
	// mark Row as deleted
	public void deleteRow(){
		tombStone = true;
	}
	
	// edit new valuer
	public void editColumnValue(int index,String newValue){
		values[index] = newValue;
	}
	
	public String getColumnValue(int index){
		return values[index];
	}
	
	//end of public methods
	
	public String toString(){
		return Arrays.toString(values);
	}
}
