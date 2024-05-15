
import java.util.*;
import java.util.stream.Collectors;
import java.io.*;
@SuppressWarnings("serial")
public class DBApp implements Serializable {
    static ArrayList<Table> tableArray = new ArrayList<Table>();
    public void init() {
		
	}
    public static void createTable(String strTableName, 
            String strClusteringKeyColumn, 
            Hashtable<String,String> htblColNameType) throws DBAppException, IOException {
            FileWriter writer = new FileWriter("C:\\Users\\MARIAM\\Desktop\\DROPTABLEteams\\data\\metadata.csv", true);
            ArrayList<String> attributes = new ArrayList<String>();
            Enumeration<String> keys = htblColNameType.keys();

            while(keys.hasMoreElements()){
                String key = keys.nextElement();
                if (Arrays.asList("java.lang.Integer", "java.lang.String", "java.lang.Double", "java.lang.Boolean", "java.util.Date").contains(htblColNameType.get(key))) {
                attributes.add(key);
                if(strClusteringKeyColumn.equals(key)){
            writer.append(strTableName + ", " + key + ", " + htblColNameType.get(key) + ", True" + System.getProperty("line.separator"));
                } else
                    writer.append(strTableName + ", " + key +", " + htblColNameType.get(key) + ", False" + System.getProperty("line.separator"));

            } else
                System.out.println("Column " + key + "'s type is invalid.");

            }

            writer.close();
            Table newTable = new Table(strTableName,attributes);
            FileOutputStream fileOut = new FileOutputStream(strTableName+1+".ser");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            Page newPage = new Page(newTable.getName());
            out.writeObject(newPage);
            out.close();
            fileOut.close();
            tableArray.add(newTable);
            newTable.addToPageArray(newPage);
            

    }
	//public void createBitmapIndex(String strTableName,
	//		String strColName) throws DBAppException {
		
	//}
	public static void insertIntoTable(String strTableName,
			Hashtable<String,Object> htblColNameValue)
			throws DBAppException, IOException, ClassNotFoundException {
		for(int i = 0; i<tableArray.size();i++)
		{
			if(tableArray.get(i).getName().equals(strTableName)) {
				tableArray.get(i).add(htblColNameValue);
				break;
			}
		}
		
	}
	public static void updateTable(String strTableName,
			String strKey,
			Hashtable<String,Object> htblColNameValue)
			throws DBAppException, FileNotFoundException, IOException, ClassNotFoundException {
		for(int i = 0; i<tableArray.size();i++)
		{
			if(tableArray.get(i).getName().equals(strTableName)) {
				tableArray.get(i).update(strKey, htblColNameValue);
				break;
			}
		}
		
	}
	public static void deleteFromTable(String strTableName,
			Hashtable<String,Object> htblColNameValue)
			throws DBAppException, FileNotFoundException, IOException {
		for(int i = 0; i<tableArray.size();i++)
		{
			if(tableArray.get(i).getName().equals(strTableName)) {
				tableArray.get(i).delete(htblColNameValue);
				break;
			}
		}
		
	}

	public static ArrayList<String> getColName() throws IOException{
		ArrayList<String> ar = null;
		ArrayList<String> tableNames = null;
		ArrayList<String> columnNames = null;
		try
		{
		ar = new ArrayList<String>();
		tableNames = new ArrayList<String>();
		columnNames = new ArrayList<String>();
		File csvFile = new File("C:\\Users\\user\\eclipse-workspace\\DROPTABLEteams\\data\\metadata.csv");
		BufferedReader br = new BufferedReader(new FileReader(csvFile));
		String line = "";
		StringTokenizer st = null;
		int lineNumber = 0;
		int tokenNumber = 0;

		while ((line = br.readLine()) != null) {
			String[] arr = line.split(",");
			tableNames.add(arr[0]);
			columnNames.add(arr[1]);
			lineNumber++;
		}
		}
		catch(IOException ex) {
		ex.printStackTrace();
		}
		List<String> columnNameNodupes = columnNames.stream().distinct().collect(Collectors.toList());
		return (ArrayList<String>) columnNameNodupes;
	}

	public static ArrayList<String> getTableName() throws IOException{
		ArrayList<String> ar = null;
		ArrayList<String> tableNames = null;
		ArrayList<String> columnNames = null;
		try
		{
		ar = new ArrayList<String>();
		tableNames = new ArrayList<String>();
		columnNames = new ArrayList<String>();
		File csvFile = new File("C:\\Users\\user\\eclipse-workspace\\DROPTABLEteams\\data\\metadata.csv");
		BufferedReader br = new BufferedReader(new FileReader(csvFile));
		String line = "";
		StringTokenizer st = null;
		int lineNumber = 0;
		int tokenNumber = 0;

		while ((line = br.readLine()) != null) {
			String[] arr = line.split(",");
			tableNames.add(arr[0]);
			columnNames.add(arr[1]);
			lineNumber++;
		}
		}
		catch(IOException ex) {
		ex.printStackTrace();
		}
		List<String> tableNameNodupes = tableNames.stream().distinct().collect(Collectors.toList());
		return (ArrayList<String>) tableNameNodupes;
	}

	public static boolean isTableName(String tNameIn) throws IOException {
		ArrayList <String> tNames = getTableName();
		boolean fTableName = true;
		for(int i=0; i<tNames.size(); i++) {
			if(!((tNames.get(i)).equals(tNameIn)))
				fTableName = false;
		}

		return fTableName;
	}

	public static boolean isColumnName(String colNameIn) throws IOException{
		ArrayList <String> colNames = getColName();
		for(int i=0; i<colNames.size(); i++) {
			if((colNames.get(i)).equals(colNameIn))
				return true;
		}
		return false;
	}

	public static boolean isValidOperator(String opIn) {
		boolean f = false;
		switch(opIn) {
		case(">"): f = true; break;
		case(">="): f = true; break;
		case("<"): f = true; break;
		case("<="): f = true; break;
		case("!="): f = true; break;
		case("="): f = true; break;
		}
		return f;
	}

	public static boolean isValidStrOperator(String opIn) {
		boolean f = false;
		switch(opIn) {
		case("OR"): f = true; break;
		case("AND"): f = true; break;
		case("XOR"): f = true; break;
		}
		return f;
	}
	//dont forget to change void back into iterator
	public static Iterator selectFromTable(SQLTerm[] arrSQLTerms,	String[] strarrOperators) throws DBAppException, IOException{
		ArrayList<Hashtable<String,Object>> resultSet = new ArrayList<Hashtable<String,Object>>();
		ArrayList<Hashtable<String,Object>> q = new ArrayList<Hashtable<String,Object>>();
		//getting all of the SQLTerms
		for(int i=0; i<arrSQLTerms.length; i++) {
			//finding if such a table name exists
			if(!(isTableName(arrSQLTerms[i]._strTableName)))
				throw new DBAppException("Wrong table name");
			else {
			//finding if such a column name exists in that table
			if(!(isColumnName(" "+arrSQLTerms[i]._strColumnName)))
				throw new DBAppException("Wrong column name");
			else {
			//finding if such an operator is valid
			if(!(isValidOperator(arrSQLTerms[i]._strOperator)))
				throw new DBAppException("Invalid Operator");
			else {

			//finding if such an objValue exists in that certain column and adding the result into a result set
			for(int j=0; j< tableArray.size(); j++) {
				if((tableArray.get(j).getName()).equals(arrSQLTerms[i]._strTableName)) {
					for(int k=0; k< tableArray.get(j).getNumberOfPages(); k++) {
						for(int l=0; l< tableArray.get(j).getPageArray().get(k).size(); l++) {

							if(
									tableArray.get(j).getPageArray().get(k).get(l).containsValue(arrSQLTerms[i]._objValue)){

								q.add(tableArray.get(j).getPageArray().get(k).get(l));
						}
						}
					}
				}
			}
			}
			}
			}
			}
		if(q.size() == 0) {
			throw new DBAppException("There are no queries matching that");
		}
		//finding the string array operators
		System.out.println(q.toString());
		for(int m=0; m<strarrOperators.length; m++) {
			//checking for a valid strarroperator
			if(!isValidStrOperator(strarrOperators[m]))
				throw new DBAppException("Invalid String Operator");
			else {
				if(strarrOperators[m].equals("AND"))
				{
					if(q.get(m).equals(q.get(m+1)))
						resultSet.add(q.get(m));
				}
				else if(strarrOperators[m].equals("OR")) {
						resultSet.add(q.get(m));
						resultSet.add(q.get(m+1));

				}
				else if(strarrOperators[m].equals("XOR")) {
					if(q.get(m).contains(q.get(m+1)) && !(q.get(m+1).contains(q.get(m))))
						resultSet.add(q.get(m));
				}
			}

		}
		return resultSet.iterator();
	}

	public static void createBitmapIndex(String strTableName,
			String strColName) throws DBAppException, IOException {
				Table indexTable = null;
				Page bitmap = new Page(strTableName);
				for(int i = 0; i<tableArray.size();i++)
					{
						if(tableArray.get(i).getName().equals(strTableName)) {
							indexTable = tableArray.get(i);
							break;
						}
					}
				//indexTable.createIndex(strColName);
				ArrayList<Page> pages = indexTable.getPageArray();
				//Finding each page
				for(int j = 0; j<pages.size();j++)
				{
					//Getting hashtables one at a time
					Page page = pages.get(j);
					for(int k = 0;k<page.size();k++)
					{
						//Getting each hashtable's relevant column value
						String value = page.get(k).get(strColName).toString();
						//Searching the bitmap for if that value occurred before
						
						Hashtable<String,Object> bitmapHashtable = null;
						int index = -1;
						for(int l= 0; l<bitmap.size();l++)
						{	
							Enumeration<String> keys = bitmap.get(l).keys();
							//These are the times when the bit should be equal to 1
							//int indexOfBitmap = -1;
							//Checking whether bitmap already contains this entry
							//If so, adjust its bitmap, else make a new entry
								if(keys.nextElement() == value) {
									bitmapHashtable = bitmap.get(l);
									index = l;
									break;
								}
							
						}
						//Assigning a 1 after the bitmap value if the bitmap already exists
						//Otherwise, adding a new hashtable with a bunch of 0s, then a 1
						if(bitmapHashtable != null)
						{
							String newBitmap = (String) bitmapHashtable.get(value) + 1;
							Hashtable<String,Object> newBitmapHashtable = new Hashtable<String,Object>();
							newBitmapHashtable.put(value, newBitmap);
							bitmap.set(index, newBitmapHashtable);
							for(int x = 0; x<bitmap.size();x++)
							{
								if(x!=index)
								{
									Enumeration<String> currentKey = bitmap.get(x).keys();
									String bitmapReplacement = (String) bitmap.get(x).get(currentKey.nextElement()) + 0;
									bitmap.get(x).replace(currentKey.nextElement(), bitmapReplacement);
								}
							}
							
						}
						else {
							int numberOfZeroes = 0;
							String zeroString = "";
							if(bitmap.size()!= 0)
							{
								numberOfZeroes = ((String) bitmap.get(0).get(bitmap.get(0).keys().nextElement())).length();
							}
							for(int y = 0;y<numberOfZeroes;y++)
							{
								zeroString += 0;
							}
							for(int x = 0; x<bitmap.size();x++)
							{
								String currentKey = bitmap.get(x).keys().nextElement();
								
								String bitmapReplacement = (String) bitmap.get(x).get(currentKey) + 0;
								bitmap.get(x).replace(currentKey, bitmapReplacement);
								
							}
							Hashtable<String,Object> newBitmapHashtable = new Hashtable<String,Object>();
							newBitmapHashtable.put(value, zeroString+1);
							bitmap.add(newBitmapHashtable);
						}
						
						
						
						
					}
				}
				indexTable.addIndexArray(bitmap);
				try {
					indexTable.sortIndexCreation();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				//Finding the table and column in the csv file
				try {
				BufferedReader metadataReader = new BufferedReader(
				        new InputStreamReader(new FileInputStream("C:\\Users\\user\\eclipse-workspace\\DROPTABLEteams\\data\\metadata.csv")));
				String line;
				
					while((line = metadataReader.readLine())!=null) {
					    if(!line.isEmpty()) {
					        if(line.substring(0, line.indexOf(',')).equals(strTableName)) {
					        	 int firstComma = line.indexOf(',');
					        	 int secondComma = line.indexOf(',',firstComma+1);
					        	 if(line.substring(firstComma+1, secondComma).equals(strColName))
					            break;
					        }
					    }
					}
					deleteMetadataLine(line);
					if(line.substring(line.length()-5,line.length()).contains("False"))
					{
						String newLine = line.substring(line.length()-5, line.length()) + "True";
			            FileWriter writer = new FileWriter("C:\\Users\\user\\eclipse-workspace\\DROPTABLEteams\\data\\metadata.csv", true);
			            writer.append(newLine + System.getProperty("line.separator"));
			            writer.close();
					}
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			public static void deleteMetadataLine(String lineToRemove)
			{
			    try {
				File inputFile = new File("C:\\\\Users\\\\user\\\\eclipse-workspace\\\\DROPTABLEteams\\\\data\\\\metadata.csv");
				File tempFile = new File("myTempFile.txt");

				BufferedReader reader = new BufferedReader(new FileReader(inputFile));
				BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile));

			
				String currentLine;

				while((currentLine = reader.readLine()) != null) {
				    // trim newline when comparing with lineToRemove
				    String trimmedLine = currentLine.trim();
				    if(trimmedLine.equals(lineToRemove)) continue;
				
						writer.write(currentLine + System.getProperty("line.separator"));
					
				}
				writer.close(); 
				reader.close(); 
				boolean successful = tempFile.renameTo(inputFile);
			    } catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
	
	
	public static void main(String[] args) throws DBAppException, IOException, ClassNotFoundException
    {
    	String strTableName = "Student";
    	Hashtable<String,String> htblColNameType = new Hashtable<String,String>( );
    	htblColNameType.put("id", "java.lang.Integer");
    	htblColNameType.put("name", "java.lang.String");
    	htblColNameType.put("gpa", "java.lang.Double");
    	createTable(strTableName, "id", htblColNameType);

    	Hashtable<String, Object> htblColNameValue4 = new Hashtable<String, Object>( );
    	htblColNameValue4.put("id", new Integer( 1 ));
    	htblColNameValue4.put("name", new String("first entry" ) );
    	htblColNameValue4.put("gpa", new Double( 1.25 ) );
    	insertIntoTable( strTableName , htblColNameValue4 );
    	
    	Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>( );
    	htblColNameValue.put("id", new Integer( 2 ));
    	htblColNameValue.put("name", new String("second entry" ) );
    	htblColNameValue.put("gpa", new Double( 1.25 ) );
    	insertIntoTable( strTableName , htblColNameValue );
    	
    	
    	
    	Hashtable<String, Object> htblColNameValue5 = new Hashtable<String, Object>( );
    	htblColNameValue5.put("id", new Integer( 3 ));
    	htblColNameValue5.put("name", new String("third entry" ) );
    	htblColNameValue5.put("gpa", new Double( 2.5 ) );
    	insertIntoTable( strTableName , htblColNameValue5 );
    	
    	

    	Hashtable<String, Object> htblColNameValue2 = new Hashtable<String, Object>( );
    	htblColNameValue2.put("id", new Integer( 4 ));
    	htblColNameValue2.put("name", new String("fourth entry" ) );
    	htblColNameValue2.put("gpa", new Double( 5.00 ) );
    	insertIntoTable( strTableName , htblColNameValue2 );
    	

 /*   	Hashtable<String, Object> update = new Hashtable<String, Object>( );
    	update.put("name", "updated");
    	update.put("gpa", 5.00);
    	update.put("id", 4);
    	updateTable(strTableName, "3", update);*/
    	
    	Hashtable<String, Object> htblColNameValue3 = new Hashtable<String, Object>( );
    	htblColNameValue3.put("id", new Integer( 5 ));
    	htblColNameValue3.put("name", new String("fifth value" ) );
    	htblColNameValue3.put("gpa", new Double( 1.25 ) );
    	insertIntoTable( strTableName , htblColNameValue3 );
/*    
    	deleteFromTable(strTableName, htblColNameValue3);
    	deleteFromTable(strTableName, htblColNameValue);

    	deleteFromTable(strTableName, htblColNameValue4);
    	deleteFromTable(strTableName, htblColNameValue2);
*/
    	createBitmapIndex(strTableName, "id");
    	System.out.println(tableArray.get(0).getPageArray().toString());
    
    	
    	
    	
    	SQLTerm[] arrSQLTerms; 
		arrSQLTerms = new SQLTerm[2];
		arrSQLTerms[0]._strTableName = "Student";
		arrSQLTerms[0]._strColumnName= "name";
		arrSQLTerms[0]._strOperator = "=";
		arrSQLTerms[0]._objValue = "Dalia Noor";

		arrSQLTerms[1]._strTableName = "Student";
		arrSQLTerms[1]._strColumnName= "gpa";
		arrSQLTerms[1]._strOperator = "=";
		arrSQLTerms[1]._objValue = new Double( 1.25 );

		String[]strarrOperators = new String[1];
		strarrOperators[0] = "OR";

		// select * from Student where name = “Dalia Noor” or gpa = 1.5;
		Iterator resultSet = selectFromTable(arrSQLTerms , strarrOperators);
		//System.out.println("ATTENTIOn");
		while(resultSet.hasNext())
        {
            System.out.println(resultSet.next());
        }

    }

}
