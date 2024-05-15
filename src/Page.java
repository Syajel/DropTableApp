import java.util.*;
import java.io.*;
@SuppressWarnings("serial")
public class Page extends Vector<Hashtable<String,Object>> implements Serializable{
	String tableName;
	private int maxNumberOfRows = 2; //N
	private int maxIndexRows = 4; //M
	
	public Page(String tableName) throws IOException{
	    this.tableName = tableName;
	    
	}
	
	



	public boolean isEmpty(){
		if(this.size()==0){
			return true;
		}
		else{
			return false;
		}
	}
	
	


	public int getMaxNumberOfRows() {
		return maxNumberOfRows;
	}
	
	public int getMaxIndexRows() {
		return maxIndexRows;
	}

	public void setMaxNumberOfRows(int maxNumberOfRows) {
		this.maxNumberOfRows = maxNumberOfRows;
	}


	public int getRowsLeft() {
		return maxNumberOfRows - this.size();
	}
	public Hashtable<String, Object> getLast() {
		return this.get(this.size()-1);
	}
	public boolean isFull() {
		return maxNumberOfRows - this.size() == 0;
	}
	public void setLast(Object object) {
		this.set(this.size()-1, null);
	}
	public void deleteLast() {
		this.remove(this.size()-1);
	}
}
	
	
	
