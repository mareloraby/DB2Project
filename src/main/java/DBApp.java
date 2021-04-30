import java.io.*;
import java.util.*;
import java.util.Map.Entry;

public class DBApp implements DBAppInterface {
    static int MaximumRowsCountinPage;
    static int MaximumKeysCountinIndexBucket;

    @Override
    public void init()  {

    }

    public ArrayList<String> getTableNames() throws IOException {
        String row;
        ArrayList<String> names = new ArrayList<>();
        BufferedReader csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
        while ((row = csvReader.readLine()) != null) {
            String[] data = row.split(",");
            names.add(data[0]);
        }
        csvReader.close();
        return names;
    }

    @Override
    public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType, Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException, IOException {
        FileWriter csvWriter = new FileWriter("src/main/resources/metadata.csv", true);
        ArrayList<String> AllTablesNames = getTableNames(); // To keep track of all tables created

        // Exceptions

        //Check Validity of column types
        String invalidCol = checkColumnTypes(colNameType);
        if (invalidCol != null)
            throw new DBAppException("Invalid column type: " + invalidCol + ".");

        // The clusteringKey is not null.
        if (clusteringKey == null || clusteringKey.equals(""))
            throw new DBAppException("The clustering key shouldn't be equal null.");

        //The table name is not null.
        if (tableName == null || tableName.equals(""))
            throw new DBAppException("The Table name shouldn't be equal null.");

        //The table name already exists.
        for (int i = 0; i < AllTablesNames.size(); i++) {
            if (tableName.equals(AllTablesNames.get(i))) {
                throw new DBAppException("The table name already exists.");
            }
        }

        AllTablesNames.add(tableName);
        boolean ck_match = false;

        Enumeration<String> keys = colNameType.keys();
        Enumeration<String> keysmin = colNameMin.keys();
        Enumeration<String> keysmax = colNameMax.keys();
        while (keys.hasMoreElements()) {
            //csvWriter.append()
            csvWriter.append(tableName);
            csvWriter.append(",");
            String colname = keys.nextElement();
            String coltype = colNameType.get(colname);

            // Exception
            // all col names and types are entered
            if (colname == null)
                throw new DBAppException("The column name should not be equal null.");
            if (coltype == null)
                throw new DBAppException("The column type should not be equal null.");

            csvWriter.append(colname);
            csvWriter.append(",");
            csvWriter.append(coltype);
            csvWriter.append(",");
            if (clusteringKey.equals(colname))
            {csvWriter.append("True"); ck_match = true;}
            else
            {csvWriter.append("False");}
            csvWriter.append(",");
            csvWriter.append("False"); //indexed
            csvWriter.append(",");

            String min = keysmin.nextElement();
            String minvalue = colNameMin.get(min);

            // Exception
            //each colname has a type as well as max and min values
            if (minvalue == null || minvalue.equals(""))
                throw new DBAppException("The column minimum value should not be equal null.");

            if (!ck_match)
                throw new DBAppException("Clustering Key entered doesn't match any colName.");

            csvWriter.append(minvalue);
            csvWriter.append(",");

            String max = keysmax.nextElement();
            String maxvalue = colNameMax.get(max);

            //Exception
            //each colname has a type as well as max and min values
            if (maxvalue == null || maxvalue.equals(""))
                throw new DBAppException("The column maximum value should not be equal null.");

            csvWriter.append(maxvalue);
            csvWriter.append("\n");


        }
        Table t = new Table(tableName);
        serialize(t, tableName);
        csvWriter.close();


    }

    // check whether types are valid
    @Override
    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException, IOException {
        ArrayList<String> AllTablesNames = getTableNames();
        if (!AllTablesNames.contains(tableName))
            throw new DBAppException("The table does not exist.");

        // String[] columnNames= get this from csv file
        String csvLine;
        ArrayList<String> colNames = new ArrayList<>();
        ArrayList<ArrayList<Object>> min_max = new ArrayList<>();
        ArrayList<String> colTypes = new ArrayList<>();
        BufferedReader csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));

        int pk_found = -1;
        boolean found = false;
        int index = 0;
        while ((csvLine = csvReader.readLine()) != null) {
            String[] data = csvLine.split(",");
            if (data[0] == tableName) {
                found = true;
                ArrayList<Object> MinMax = new ArrayList<>();
                colTypes.add(data[2]);
                MinMax.add(data[5]);
                MinMax.add(data[6]);
                min_max.add(MinMax);
                colNames.add(data[1]);
                if (data[3].equals("True") || data[3].equals("true")) pk_found = index;
                index++;
            } else if (data[0] != tableName && found == true)
                break;
        }
        csvReader.close();

        if (pk_found == -1) throw new DBAppException("No Primary Key inserted");

        // move from values array to values Vector
        Vector<Object> row = new Vector<Object>();

        for (int i = 0; i < colNames.size(); i++) {
            Object value = colNameValue.get(colNames.get(i));
            if (Trial.compare(value, min_max.get(i).get(0)) == -1 && Trial.compare(value, min_max.get(i).get(1)) == 1)
                throw new DBAppException("The inserted value is not within the min and max value range. ");

            if (colTypes.get(i) != colNameValue.get(colNames.get(i)).getClass().getName())
                throw new DBAppException("The inserted value is not of the right type. ");

            row.add(colNameValue.get(colNames.get(i)));
        }

        Table t = (Table) DBApp.deserialize(tableName);
        t.insertIntoPage(row, pk_found);
        serialize(t, tableName);

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

    //table columns:<gpa,id,date>
    @Override //hastable parameter: <gpa,2> <id,1>
    public void deleteFromTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException, IOException {
        /* 1. Search for the record to be deleted
         * 2. If the search key is a primary key, do binary search
         * 3. If search key is not a primary key, do linear search
         * 4. Delete the record
         * 5. If this record was the last record, delete the page.   */

        ArrayList<String> AllTablesNames = getTableNames();
        if (!AllTablesNames.contains(tableName))
            throw new DBAppException("The table does not exist.");

        // String[] columnNames= get this from csv file
        String csvLine;
        ArrayList<String> colNames = new ArrayList<>();
        ArrayList<ArrayList<Object>> min_max = new ArrayList<>();
        ArrayList<String> colTypes = new ArrayList<>();
        BufferedReader csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));

        int pk_found = -1;
        String pk_colName = "";
        boolean found = false;
        int index = 0;
        while ((csvLine = csvReader.readLine()) != null) {
            String[] data = csvLine.split(",");
            if (data[0] == tableName) {
                found = true;
                ArrayList<Object> MinMax = new ArrayList<>();
                colNames.add(data[1]);
                colTypes.add(data[2]);
                MinMax.add(data[5]);
                MinMax.add(data[6]);
                min_max.add(MinMax);
                if (data[3].equals("True") || data[3].equals("true")) {
                    pk_found = index; //found primary key index
                    pk_colName = data[1]; // primary key column name
                }
                index++; //index of primary key in columns of a table
            } else if (data[0] != tableName && found == true)
                break;
        }
        csvReader.close();

        boolean do_BS = false;
//        if (pk_found == -1) throw new DBAppException("No Primary Key inserted");
        Object pk_value = colNameValue.get(pk_colName);
        if (pk_value != null)
            do_BS = true;


        // move from values array to values Vector
        // stores index with its value
        Vector<Vector> index_value = new Vector<Vector>();

        for (int i = 0; i < colNames.size(); i++) {
            Object value = colNameValue.get(colNames.get(i));
            if (value != null) { //check if value within right range
                if (Trial.compare(value, min_max.get(i).get(0)) == -1 && Trial.compare(value, min_max.get(i).get(1)) == 1)
                    throw new DBAppException("Value is not within the min and max value range. ");
                if (colTypes.get(i) != colNameValue.get(colNames.get(i)).getClass().getName())
                    throw new DBAppException("Value is not of the right type. ");
                Vector<Object> v= new Vector<Object>();
                v.add(i);//??
                v.add(value);
                index_value.add(v);
            }
        }

        Table t = (Table) DBApp.deserialize(tableName);
        t.deleteFromPage(index_value, pk_found, pk_value);
        serialize(t, tableName);

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
        HashSet<String> dataTypes = new HashSet<String>();
        dataTypes.add("java.lang.Integer");
        dataTypes.add("java.lang.String");
        dataTypes.add("java.lang.Date");
        dataTypes.add("java.lang.Double");


        for (Entry<String, String> entry : colNameType.entrySet())
            if (!dataTypes.contains(entry.getValue()))
                return entry.getKey();
        return null;
    }

    public static void serialize(Object e, String fileName) {
        try {
            FileOutputStream fileOut =
                    new FileOutputStream("src/main/resources/data" + fileName + ".class");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(e);
            out.close();
            fileOut.close();
            System.out.printf("Serialized data is saved in " + fileName + ".class");
        } catch (IOException i) {
            i.printStackTrace();
        }

    }

    public static Object deserialize(String fileName) {
        try {
            FileInputStream fileIn = new FileInputStream("src/main/resources/data" + fileName + ".class");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            Object e = in.readObject();
            in.close();
            fileIn.close();
            return e;
        } catch (IOException i) {
            i.printStackTrace();
            return null;
        } catch (ClassNotFoundException c) {
            System.out.println("not found");
            c.printStackTrace();
            return null;
        }


    }


    public int getPropValues(String string) throws IOException {
        Properties prop = new Properties();
        FileInputStream inputStream = new FileInputStream("src/main/resources/DBApp.config");
        prop.load(inputStream);
        return Integer.parseInt(prop.getProperty(string));
    }


    public static void main(String[] args) throws DBAppException, IOException {
        Hashtable htblColNameValue = new Hashtable();
        String strTableName = "Yes";
        DBApp dbApp = new DBApp();
        //      htblColNameValue.put("id", new Integer( 453455 ));
        htblColNameValue.put("id", Integer.valueOf(453455)); // fixed the "dashed" Integer elkan 3amlha 3shan kan metal3 error

        htblColNameValue.put("name", new String("Ahmed Noor"));
//        htblColNameValue.put("gpa", new Double( 0.95 ) );
        htblColNameValue.put("gpa", Double.valueOf(0.95));

        //   dbApp.insertIntoTable(strTableName , htblColNameValue );


        try {
            MaximumRowsCountinPage = dbApp.getPropValues("MaximumRowsCountinPage");
            MaximumKeysCountinIndexBucket = dbApp.getPropValues("MaximumKeysCountinIndexBucket");

        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(MaximumRowsCountinPage + "");
        Hashtable htblColNameType = new Hashtable();
        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.double");
        Hashtable min = new Hashtable();
        min.put("id", "0");
        min.put("name", "A");
        min.put("gpa", "0");
        Hashtable max = new Hashtable();
        max.put("id", "10000");
        max.put("name", "ZZZZZZZZZZZ");
        max.put("gpa", "1000000");
        //   dbApp.createTable("T1","id", htblColNameType , min , max );
        Table t = (Table) deserialize("T1");
        System.out.println(t.getCount());
//        insertIntoTable(t.getTableName(), )

    }


}
