import java.util.*;
import java.io.*;
@SuppressWarnings("serial")
public class Table implements Serializable {
		private String strTableName;
	    private ArrayList<Page> pageArray = new ArrayList<Page>();
	    private ArrayList<Page> indexArray = new ArrayList<Page>();
		private ArrayList<String> attributes;
		private String primaryKeyCol = "";
		private String indexCol = "";
		int numberOfPages = 1;
		
		int numberOfRows = 0;

		Table(String strTableName,ArrayList<String> attributes) {
			this.strTableName = strTableName;
			this.attributes = attributes;
		}
		// helper method for the insertion
		@SuppressWarnings({ "unused", "unchecked" })
		public void add(Hashtable<String,Object> entry) throws IOException, ClassNotFoundException {
			this.primaryKeyCol = checkAgainstMetaData(entry);
			if(primaryKeyCol.equals("error")) {
				System.out.print("Wrong Data Type");
				return;
			}
			String primaryKeyVal = entry.get(primaryKeyCol).toString();
			for(Page x : pageArray) {
				for(Hashtable<String, Object> y : x) {
					if((y.get(primaryKeyCol).toString().equals(primaryKeyVal))){
						System.out.println("Error: Primary Key Already Found.");
						return;
					}
				}
			}
			if(pageArray.get(numberOfPages-1).isFull()) {
				Page newPage = new Page(strTableName);
				pageArray.add(newPage);
				newPage.add(entry);
				numberOfPages++;
			}
			else {
				pageArray.get(numberOfPages-1).add(entry);
			}
			this.sort();
			if(!indexCol.isEmpty()) {
				String indexKeyVal = entry.get(indexCol).toString();
				boolean foundIndex = false;
				for(Page x : indexArray) {
					for(int k = 0 ; k < x.size() ; k++) {
						if(x.get(k).containsKey(indexKeyVal)) {
							foundIndex = true;
							String oldBitmap = x.get(k).get(indexKeyVal).toString();
							x.get(k).replace(indexKeyVal, oldBitmap+'1');
						}
					}
				}
				if(foundIndex) {
					for(Page x : indexArray) {
						for(Hashtable<String, Object> y : x) {
							y.forEach((k, v) -> { 
								if(k != indexKeyVal) {
									y.replace(k, ((String)v+'0'));
								}
					        });
						}
					}
				}else {
					String newIndexBitmap = "";
					for(Page x : pageArray) {
						for(Hashtable<String, Object> y : x) {
							if(y.get(indexCol).toString().equals(indexKeyVal)) {
								newIndexBitmap += '1';
							}
							else {
								newIndexBitmap += '0';
							}
						}
					}
					Hashtable<String, Object> indexEntry = new Hashtable<String, Object>();
					indexEntry.put(indexKeyVal, newIndexBitmap);
					if(!indexArray.get(indexArray.size()-1).isFull()) {
						indexArray.get(indexArray.size()-1).add(indexEntry);
					}else {
						Page newPage = new Page(strTableName);
						indexArray.add(newPage);
						newPage.add(indexEntry);
					}
					this.sortIndexInsertion();
				}
			}
			System.out.println(pageArray.toString());		//testing
		}
		
		
		void sortIndexCreation() throws IOException {
			for(Page x : indexArray) {
				if(x.size() > x.getMaxIndexRows()) {
						Page newIndexPage = new Page(strTableName);
					for(int i = x.getMaxIndexRows() ; i <= x.size() ; i++) {
						newIndexPage.add(x.get(i));
					}
					indexArray.add(newIndexPage);
					x.setSize(x.getMaxIndexRows());
				}
				Collections.sort(x, new Comparator<Object>() {
					  @SuppressWarnings("unchecked")
					public int compare(Object a, Object b) {
					    return (((Hashtable<String, Object>)a).keys().toString()).compareTo(((Hashtable<String, Object>)b).keys().toString());
					  }
					});
				try {
					FileOutputStream pageFile = new FileOutputStream(strTableName+"Index"+(indexArray.indexOf(x)+1)+".ser");
					ObjectOutputStream outputFile = new ObjectOutputStream(pageFile);
					outputFile.writeObject(x);
					outputFile.close();
					pageFile.close();
					} catch(IOException e) {
						System.out.print("File Not Found");
					}
			}
			
		}

		
		private void sortIndexInsertion() {
			Hashtable<String, Object> lastEntry = indexArray.get(indexArray.size()-1).getLast();
			for(Page x : indexArray) {
				int pageSize = x.size();
				for(int i = 0 ; i < pageSize ; i++){
					try {
						if((x.get(i)==null)||(lastEntry.keys().toString().compareTo(x.get(i).keys().toString()) < 0)) {
							x.insertElementAt(lastEntry, i);
							break;
						}
					}
					catch(NullPointerException e){
						return;
					}
				}
			}
			
			boolean insertionFlag = false;
			Hashtable<String, Object> htblMoved = null;
			for(Page x : indexArray) {
				if(insertionFlag) {								//inserting overflow from last page
					x.insertElementAt(htblMoved, 0);
					insertionFlag = false;
					
				}
				if(x.size() > x.getMaxIndexRows()) {				//overflow in page
					htblMoved = x.getLast();
					indexArray.get(indexArray.size()-1).deleteLast();
					x.setSize(x.getMaxIndexRows());
					insertionFlag = true;
				}
				try {
					FileOutputStream pageFile = new FileOutputStream(strTableName+"Index"+(indexArray.indexOf(x)+1)+".ser");
					ObjectOutputStream outputFile = new ObjectOutputStream(pageFile);
					outputFile.writeObject(x);
					outputFile.close();
					pageFile.close();
					} catch(IOException e) {
						System.out.print("File Not Found");
					}
			}
			
		
		}
		
		
		private void sort() {
			Hashtable<String, Object> lastEntry = pageArray.get(pageArray.size()-1).getLast();
			for(Page x : pageArray) {
				int pageSize = x.size();
				for(int i = 0 ; i < pageSize ; i++){
					try {
						if((x.get(i)==null)||(lastEntry.get(primaryKeyCol).toString().compareTo(x.get(i).get(primaryKeyCol).toString()) < 0)) {
							x.insertElementAt(lastEntry, i);
							break;
						}
					}
					catch(NullPointerException e){
						return;
					}
				}
			}
			boolean insertionFlag = false;
			Hashtable<String, Object> htblMoved = null;
			for(Page x : pageArray) {
				if(insertionFlag) {								//inserting overflow from last page
					x.insertElementAt(htblMoved, 0);
					insertionFlag = false;
					
				}
				if(x.size() > x.getMaxNumberOfRows()) {				//overflow in page
					htblMoved = x.getLast();
					pageArray.get(pageArray.size()-1).deleteLast();
					x.setSize(x.getMaxNumberOfRows());
					insertionFlag = true;
				}
				try {
					FileOutputStream pageFile = new FileOutputStream(strTableName+(pageArray.indexOf(x)+1)+".ser");
					ObjectOutputStream outputFile = new ObjectOutputStream(pageFile);
					outputFile.writeObject(x);
					outputFile.close();
					pageFile.close();
					} catch(IOException e) {
						System.out.print("File Not Found");
					}
			}
			
		}

		public String checkAgainstMetaData(Hashtable<String, Object> entry) throws FileNotFoundException, IOException {
			Enumeration<String> keys = entry.keys();
			@SuppressWarnings("resource")
			BufferedReader metadataReader = new BufferedReader(
			        new InputStreamReader(new FileInputStream("C:\\Users\\MARIAM\\Desktop\\DROPTABLEteams\\data\\metadata.csv")));
			String line;
			while((line = metadataReader.readLine())!=null) {
                if(!line.isEmpty()) {
	                if(line.substring(0, line.indexOf(',')).equals(strTableName)) {
	                    break;
	                }
                }
            }
			if(line==null) {
				System.out.print("Table Not Found");
				return "error";
			}
			while(keys.hasMoreElements()){
				if(line==null) {
                    System.out.print("Column Not Found - End Of Table"); //in case the user puts in more columns and the file has ended.
                    return "error";
                }
                String colName = keys.nextElement();
                String valueType = entry.get(colName).getClass().getName();
                int firstComma = line.indexOf(',');
                int secondComma = line.indexOf(',',firstComma+1);
                int thirdComma = line.indexOf(',',secondComma+1);
                int fourthComma = line.indexOf(',',thirdComma+1);
                if(line.substring(thirdComma+2,thirdComma+6).equals("True")) primaryKeyCol = line.substring(firstComma+2,secondComma);
                if(line.substring(fourthComma+2,fourthComma+6).equals("True")) indexCol = line.substring(firstComma+2,secondComma);
                if(!line.substring(firstComma+2,secondComma).equals(colName)) {
                    System.out.print("Column Not Found");
                    return "error";
                }
                if(!line.substring(secondComma+2, thirdComma).equals(valueType)) {
                    System.out.print("Wrong Data Type Entered");
                    return "error";
                }
                line = metadataReader.readLine();
                //csv file to check the type and put it into a string to add to oldpage
			}
			return primaryKeyCol;
		}

		public void update(String strKey,Hashtable<String,Object> newValue) throws FileNotFoundException, IOException, ClassNotFoundException{
            String clusteringKey= this.primaryKeyCol;

			//String primaryKeyVal = entry.get(primaryKeyCol).toString();
			for(Page x : pageArray) {
				for(Hashtable<String, Object> y : x) {
					if((y.get(primaryKeyCol).toString().equals(clusteringKey))){
						System.out.println("Error: Primary Key Already Found.");
						return;
					}
				}
			}
            System.out.println(clusteringKey+"is the key");
            if(!checkAgainstMetaData(newValue).equals("error")) {
            	
            
           /*if(primarykey.indexed){
                //get page number =i
                for(int i=0;i<this.indexArray.legth();i++){
                 Page page = this.indexArray.get(i);
                for(int j=0;j<page.size();j++){
					System.out.println("now in row no"+j);
					Hashtable<String,Object> currentObject= page.get(j);
					
                
                }
                
                
                init i= get i from the index;
                Page page = this.pageArray.get(i);
				int j = row number;
				Hashtable<String,Object> currentObject= page.get(j);
					Set<String> keys = currentObject.keySet();
					for(String key :keys){
						System.out.println(key.toString());
						if(newValue.containsKey(key)){
							currentObject.put(key,newValue.get(key));
						}
						System.out.println(currentObject.toString());
					}
					
					}
				}
            }
            
            else{
            
            */
            for(int i=0;i<this.numberOfPages;i++){
           	 System.out.println("now in page"+i);
			 Page page = this.pageArray.get(i);
				for(int j=0;j<page.size();j++){
					System.out.println("now in row no"+j);
					Hashtable<String,Object> currentObject= page.get(j);
					System.out.println(currentObject.get(clusteringKey));
					System.out.println(strKey);
					if(currentObject.get(clusteringKey).toString().equals(strKey)){
						System.out.println("Target Value object is"+currentObject.toString());
						Set<String> keys = currentObject.keySet();
					for(String key :keys){
						System.out.println(key.toString());
						if(newValue.containsKey(key)){
							currentObject.put(key,newValue.get(key));
						}
					}
					System.out.println("updated value is "+currentObject.toString());
					if(newValue.containsKey(clusteringKey)) {
						Hashtable<String,Object> updatedValue= new Hashtable<String,Object>();
						updatedValue=(Hashtable<String, Object>)currentObject.clone();
						this.delete(currentObject);
						this.add(updatedValue);
						
					   }
					return;	}
					
				}
			}
            // }
		}
            else {
            	//throw wrong input file exception
            }
            
		}
		
		
		public void delete(Hashtable<String,Object> targetValue) throws FileNotFoundException, IOException{
			String primaryKeyCol = checkAgainstMetaData(targetValue);
			if(!primaryKeyCol.equals("error")) {
				/*if(primarykey.indexed){
                Page page = this.index
                init i= get i from the index;
                Page page = this.pageArray.get(i);
				System.out.println("content of page currently is "+page.toString()); 
				for(int j=0;j<page.size();j++){
					Hashtable<String,Object> currentObject= page.get(j);
					if(currentObject.equals(targetValue)){
						page.remove(j);
						System.out.println("item removed");
						System.out.println("content of page is now "+page.toString());
					   if(page.isEmpty()&& !page.isFirstPage()){
						    this.deletePage(page.getPageNumber());
							System.out.println("page is empty and not the first one");
					        return;
					   }
					}
				}
            }
            else{
            
            */ 
			for(int i=0;i<this.numberOfPages;i++){
				Page page = this.pageArray.get(i);
				System.out.println("content of page currently is "+page.toString()); 
				for(int j=0;j<page.size();j++){
					Hashtable<String,Object> currentObject= page.get(j);
					if(currentObject.equals(targetValue)){
						page.remove(j);
						System.out.println("item removed");
						System.out.println("content of page is now "+page.toString());
					   if(page.isEmpty()&& this.getNumberOfPages()!=1){
						    this.deletePage(i);
							System.out.println("page is empty and not the first one");
					        return;
					   }
					}
				}
			}
			   //}
			
		}
			else {
				//throw dbapperror
			}
			
		}
		
		public void deletePage(int pageNumber) throws IOException{
			//delete the page then
			System.out.println("trying to delete the page");
			int max =this.numberOfPages;
			File deletedPage = new File("C:\\Users\\user\\eclipse-workspace\\DROPTABLEteams\\"+strTableName+(pageNumber+1)+".ser");
			this.pageArray.remove(pageNumber);
			//fix the directory of the page on ur pc bitte
			System.out.println(deletedPage.getName());
			deletedPage.delete();
			
	         
			if(pageNumber!= this.pageArray.size())
		
			for(int i = pageNumber;i<max;i++){
				System.out.println(i);
				String oldFileName = "C:\\Users\\user\\eclipse-workspace\\DROPTABLEteams\\"+strTableName+(pageNumber+1)+".ser";// add the file path?
				String newFileName = this.strTableName+ (i-1)+".ser";   // add the file path?
				File pageToBeRenamed = new File(oldFileName);
				File newPageName = new File(newFileName);
				if (pageToBeRenamed.exists()){
					pageToBeRenamed.renameTo(newPageName);
				}
				else{
					System.out.print("could not find item, last item renamed was no" + i);
					return;
				}
			}
				}
		
		
		public int getNumberOfPages() {
			return numberOfPages;
		}
		public String getName() {
			return strTableName;
		}
		public ArrayList<String> getAttributes() {
			return attributes;
		}
		public ArrayList<Page> getPageArray() {
			return pageArray;
		}
		public void addToPageArray(Page page) {
			this.pageArray.add(page);
		}
		public void addIndexArray(Page bitmap) {
			indexArray.add(bitmap);
		}
}
