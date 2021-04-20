import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;


public class DBApp implements DBAppInterface{

    private HashSet<String> dataTypes;

    @Override
    public void init() throws IOException { }

    @Override
    public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType, Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException, IOException {
        FileWriter csvWriter = new FileWriter("src/main/resources/metadata.csv", true);
        ArrayList<String> AllTablesNames = new ArrayList<>(); // To keep track of all tables created
        // Exceptions


        //
        String invalidCol = checkColumnTypes(colNameType);
        if(invalidCol != null)
            throw new DBAppException("Invalid column type: "+invalidCol +".");


        // The clusteringKey is not null.
        if(clusteringKey.equals(null) || clusteringKey.equals("") )
            throw new DBAppException("The clustering key shouldn't be equal null.");

        //The table name is not null.
        if(tableName.equals(null) || tableName.equals("") )
            throw new DBAppException("The Table name shouldn't be equal null.");



        //The table name already exists.

        for(int i=0; i<AllTablesNames.size(); i++){
            if(tableName.equals(AllTablesNames.get(i))){
                throw new DBAppException("Table name already exists.");}}

        AllTablesNames.add(tableName);

        Enumeration<String> keys = colNameType.keys();
        Enumeration<String> keysmin = colNameMin.keys();
        Enumeration<String> keysmax = colNameMax.keys();
        while( keys.hasMoreElements() )
        {
            //csvWriter.append()
            csvWriter.append(tableName);
            csvWriter.append(",");
            String colname = keys.nextElement() ;
            String coltype = colNameType.get(colname);

            // Exception
            // all col names and types are entered
            if (colname.equals(null))
                throw new DBAppException("The column name should not be equal null.");
            if (coltype.equals(null))
                throw new DBAppException("The column type should not be equal null.");

            csvWriter.append(colname);
            csvWriter.append(",");
            csvWriter.append(coltype);
            csvWriter.append(",");
               if(clusteringKey.equals(colname))
                  csvWriter.append("True");
               else
                   csvWriter.append("False");
            csvWriter.append(",");
            csvWriter.append("False"); //indexed
            csvWriter.append(",");

            String min = keysmin.nextElement();
            String minvalue = colNameMin.get(min);

            // Exception
            //each colname has a type as well as max and min values
            if (minvalue.equals(null) || minvalue.equals(""))
                throw new DBAppException("The column minimum value should not be equal null.");

            csvWriter.append(minvalue);
            csvWriter.append(",");

            String max = keysmax.nextElement();
            String maxvalue = colNameMax.get(max);

            // Exception
            //each colname has a type as well as max and min values
            if (maxvalue.equals(null) || maxvalue.equals(""))
                throw new DBAppException("The column maximum value should not be equal null.");

            csvWriter.append(maxvalue);
            csvWriter.append("\n");

        }
        csvWriter.close();

    }

    @Override
    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
// do we insert the rows as a hashtable or an arraylist? vector!

        /*
        - check whether there is an existing table with the same name.
        - check the data types of each column name in the hashtable(compare them with
        the corresponding table's column types in the metadata.csv file).
        - access table class.
        - access the pages arraylist of the table and check if there is space.
        - if no page, create a new page.
        - binary search for a suitable position to insert a page. <--
        - insert :)
         */
    }

    @Override
    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException {

    }

    @Override
    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {

    }

    @Override
    public void createIndex(String tableName, String[] columnNames) throws DBAppException {

    }
    @Override
    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        return null;
    }



    private String checkColumnTypes(Hashtable<String, String> colNameType) //check if user entered correct type while creating table
    {

        dataTypes = new HashSet<String>();

        dataTypes.add("java.lang.Integer");
        dataTypes.add("java.lang.String");
        dataTypes.add("java.lang.Date");
        dataTypes.add("java.lang.double");

        for(Entry<String, String> entry: colNameType.entrySet())
            if(!dataTypes.contains(entry.getValue()))
                return entry.getKey();
        return null;
    }


    public static void main(String[] args) throws DBAppException {
        Hashtable htblColNameValue = new Hashtable( );
        String strTableName= "Yes";
        DBApp dbApp= new DBApp();
        htblColNameValue.put("id", Integer.valueOf( 453455 )); // fixed the "dashed" Integer elkan 3amlha 3shan kan metal3 error
        htblColNameValue.put("name", new String("Ahmed Noor" ) );
        htblColNameValue.put("gpa", Double.valueOf( 0.95 ) );
        dbApp.insertIntoTable(strTableName , htblColNameValue );
    }
}
