import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.util.*;
import java.util.Properties;
import java.util.Map.Entry;

public class DBApp implements DBAppInterface {
    static int MaximumRowsCountinPage;
    static int MaximumKeysCountinIndexBucket;
    public static String types[] = {"java.lang.Integer", "java.lang.String",
            "java.lang.Double", "java.util.Date"};

    @Override
    public void init() {
    }

//    public static void main(String[] args) {
//
//
//        Page p = (Page) deserialize("courses-1");
//
//        for (int i = 0; i < p.getRows().size(); i++) {
//            System.out.println(p.getRows().get(i).toString());
//        }
//
//
////        serialize(p, "courses-1");
////        Page p3 = (Page) deserialize("courses-1.1");
////        for (int i = 0; i < p3.getRows().size(); i++) {
////            System.out.println(p.getRows().get(i).toString());
////        }
//        Page p1 = (Page) deserialize("courses-2");
//        for (int i = 0; i < p1.getRows().size(); i++) {
//            System.out.println(p1.getRows().get(i).toString());
//        }
////        Page p12 = (Page) deserialize("courses-2.1");
////        for (int i = 0; i < p12.getRows().size(); i++) {
////            System.out.println(p.getRows().get(i).toString());
////        }
////        Page p2 = (Page) deserialize("courses-3");
////        for (int i = 0; i < p2.getRows().size(); i++) {
////            System.out.println(p.getRows().get(i).toString());
////        }
//
//    }

    DBApp() {


        File f = new File("src/main/resources/data");
        if (f.exists()) {
        } else {
            // check if the directory can be created
            // using the specified path name
            if (f.mkdir() == true) {
                System.out.println("Directory has been created successfully");
            } else {
                System.out.println("Directory cannot be created");
            }
        }

        try {
            MaximumRowsCountinPage = getPropValues("MaximumRowsCountinPage");
            MaximumKeysCountinIndexBucket = getPropValues("MaximumKeysCountinIndexBucket");

        } catch (IOException e) {
            e.printStackTrace();
        }


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
        int index = 0;
        int pk_index = 0;
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
            if (clusteringKey.equals(colname)) {
                csvWriter.append("True");
                pk_index = index;
                ck_match = true;
            } else {
                csvWriter.append("False");
            }
            csvWriter.append(",");
            csvWriter.append("False"); //indexed

            csvWriter.append(",");

            String min = keysmin.nextElement();
            String minvalue = colNameMin.get(min);

            // Exception
            //each colname has a type as well as max and min values
            if (minvalue == null || minvalue.equals(""))
                throw new DBAppException("The column minimum value should not be equal null.");

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
            index++;


        }
        if (!ck_match) {
            throw new DBAppException("Clustering Key entered doesn't match any colName.");
        }
        csvWriter.close();

        Table t = new Table(tableName, pk_index);
        t.setColNamesTable(getColNamesOfTable(tableName));
        serialize(t, tableName);


    }

    @Override
    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException, IOException {
        ArrayList<String> AllTablesNames = getTableNames();
        if (!AllTablesNames.contains(tableName))
            throw new DBAppException("The table does not exist.");

        String csvLine;
        // names of columns with the order
        ArrayList<String> colNames = new ArrayList<>();
        ArrayList<ArrayList<Object>> min_max = new ArrayList<>();
        ArrayList<String> colTypes = new ArrayList<>();
        BufferedReader csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));

        int pk_found = -1;
        boolean found = false;
        int index = 0;
        while ((csvLine = csvReader.readLine()) != null) {

            String[] data = csvLine.split(",");
            if (data[0].equals(tableName)) {
                found = true;
                ArrayList<Object> MinMax = new ArrayList<>();
                colTypes.add(data[2]);

                try {
                    MinMax.add(parse(data[2], data[5]));
                    MinMax.add(parse(data[2], data[6]));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                min_max.add(MinMax);
                colNames.add(data[1]);
                if (data[3].equals("True") || data[3].equals("true")) {
                    pk_found = index;
                }
                index++;


            } else if (!data[0].equals(tableName) && found == true)
                break;

        }

        csvReader.close();

        if (pk_found == -1) throw new DBAppException("No Primary Key inserted");

        // move from values array to values Vector
        Vector<Object> row = new Vector<Object>();

        for (Entry<String, Object> entry : colNameValue.entrySet())
            if (!colNames.contains(entry.getKey()))
                throw new DBAppException("The table does not contain this column.");

        for (int i = 0; i < colNames.size(); i++) {

            Object value = colNameValue.get(colNames.get(i));
            if (((Comparable) value).compareTo((Comparable) min_max.get(i).get(0)) < 0 || ((Comparable) value).compareTo((Comparable) min_max.get(i).get(1)) > 0)
                throw new DBAppException("The inserted value is not within the min and max value range. ");

            if (!(colTypes.get(i).equals(colNameValue.get(colNames.get(i)).getClass().getName())))
                throw new DBAppException("The inserted value is not of the right type. ");

            row.add(colNameValue.get(colNames.get(i)));
            // <May, null, 9> we can add nulls
        }

        Table t = (Table) DBApp.deserialize(tableName);

        // check whether we have indices and choose the most reasonable index
        if (t.isHasGrid()) {
            t.insertIntoPageWithGI(row, pk_found, colNameValue);
        } else {
            t.insertIntoPage(row, pk_found);
        }
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
    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException, IOException {
        /* 1. Search for the record to be updated
         * 2. Use the clustering key to do binary search
         * 3. Update the record
         */


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
        String pkType = "";
        boolean found = false;
        int index = 0;
        while ((csvLine = csvReader.readLine()) != null) {
            String[] data = csvLine.split(",");
            if (data[0].equals(tableName)) {
                found = true;
                ArrayList<Object> MinMax = new ArrayList<>();
                colNames.add(data[1]);
                colTypes.add(data[2]);
//                MinMax.add(data[5]);
//                MinMax.add(data[6]);

                try {
                    MinMax.add(parse(data[2], data[5]));
                    MinMax.add(parse(data[2], data[6]));
                } catch (Exception e) {
                    e.printStackTrace();
                }

                min_max.add(MinMax);
                if (data[3].equals("True") || data[3].equals("true")) {
                    pkType = data[2];
                    pk_found = index; //found primary key index
                    pk_colName = data[1]; // primary key column name
                }
                index++; //index of primary key in columns of a table
            } else if (!data[0].equals(tableName) && found == true)
                break;
        }
        csvReader.close();

        for (Entry<String, Object> entry : columnNameValue.entrySet())
            if (!colNames.contains(entry.getKey()))
                throw new DBAppException("The table does not contain this column.");
        // move from values array to values Vector
        // stores index with its value
        //convert hashtable to vector<vector>
        Vector<Vector> index_value = new Vector<Vector>();

        for (int i = 0; i < colNames.size(); i++) {
            Object value = columnNameValue.get(colNames.get(i));
            if (value != null) { //check if value within right range
                if (((Comparable) value).compareTo((Comparable) min_max.get(i).get(0)) < 0 || ((Comparable) value).compareTo((Comparable) min_max.get(i).get(1)) > 0)
                    throw new DBAppException("Value is not within the min and max value range. ");
                if (!colTypes.get(i).equals(columnNameValue.get(colNames.get(i)).getClass().getName()))
                    throw new DBAppException("Value is not of the right type. ");
                Vector<Object> v = new Vector<Object>();
                v.add(i);
                v.add(value);
                index_value.add(v);
            }
        }


        Table t = (Table) DBApp.deserialize(tableName);

        if (t.isHasGrid()) {
            t.updateInPagewithIndex(columnNameValue, parse(pkType, clusteringKeyValue), pk_found, index_value);

            //choose index done
            //find cell done
            //update values in entries of bucket if exists in index
            //update in page
            //(delete,insert) to update in all indices
        } else {
            t.updateInPage(index_value, pk_found, parse(pkType, clusteringKeyValue));
            serialize(t, tableName);
        }


    }

    @Override
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
            if (data[0].equals(tableName)) {
                found = true;
                ArrayList<Object> MinMax = new ArrayList<>();
                colNames.add(data[1]);
                colTypes.add(data[2]);


                try {
                    MinMax.add(parse(data[2], data[5]));
                    MinMax.add(parse(data[2], data[6]));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                min_max.add(MinMax);
                if (data[3].equals("True") || data[3].equals("true")) {
                    pk_found = index; //found primary key index
                    pk_colName = data[1]; // primary key column name
                }
                index++; //index of primary key in columns of a table
            } else if (!data[0].equals(tableName) && found == true)
                break;
        }
        csvReader.close();

        boolean do_BS = false;
        Object pk_value = colNameValue.get(pk_colName);
        if (pk_value != null)
            do_BS = true;

        for (Entry<String, Object> entry : colNameValue.entrySet())
            if (!colNames.contains(entry.getKey()))
                throw new DBAppException("The table does not contain this column.");

        // move from values array to values Vector
        // stores index with its value
        Vector<Vector> index_value = new Vector<Vector>();

        for (int i = 0; i < colNames.size(); i++) {
            Object value = colNameValue.get(colNames.get(i));
            if (value != null) { //check if value within right range
                if (Trial.compare(value, min_max.get(i).get(0)) == -1 && Trial.compare(value, min_max.get(i).get(1)) == 1)
                    throw new DBAppException("Value is not within the min and max value range. ");
                if (!colTypes.get(i).equals(colNameValue.get(colNames.get(i)).getClass().getName()))
                    throw new DBAppException("Value is not of the right type. ");
                Vector<Object> v = new Vector<Object>();
                v.add(i);//??
                v.add(value);
                index_value.add(v);
            }
        }


        //query: <name = ali, age = 14, id = pk3>
        // index: <name = ali, age = 14>

        //<pk1,pageName,colname1,colname2,...>
        //<pk2,pageName,colname1,colname2,...>

        Table t = (Table) DBApp.deserialize(tableName);
        if (t.isHasGrid()) {
            t.deleteUsingIndex(colNameValue, pk_value, pk_found, index_value);
        } else
            t.deleteFromPage(index_value, pk_found, pk_value);

        serialize(t, tableName);
    }

    @Override
    public void createIndex(String tableName, String[] columnNames) throws DBAppException, IOException {
        String csvLine;
        BufferedReader csvReader = null;
        ArrayList<String> colNames = new ArrayList<>();


        ArrayList<String> AllTablesNames = getTableNames();

        if (!AllTablesNames.contains(tableName)) {
            throw new DBAppException("The table does not exist.");
        }
        try {
            csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        Table t = (Table) DBApp.deserialize(tableName);
        //name, age, gpa
        Vector<GridIndex> gridIndices = t.getGridIndices();
        for (int k = 0; k < gridIndices.size(); k++) {

            if (Arrays.equals(gridIndices.get(k).getColNames(), columnNames)) {
                throw new DBAppException("There is already an existing index.");
            }
        }

        StringBuilder sb = new StringBuilder();
        while ((csvLine = csvReader.readLine()) != null) {

            String[] data = csvLine.split(",");
            if (data[0].equals(tableName)) {

                colNames.add(data[1]);
                for (int i = 0; i < columnNames.length; i++) {
                    if (data[1].equals(columnNames[i])) {
                        data[4] = "True";
                        break;
                    }
                }
                sb.append(data[0]);
                for (int i = 1; i < data.length; i++) {
                    sb.append(",");
                    sb.append(data[i]);
                }
                sb.append("\n");

            } else if (!data[0].equals(tableName)) {

                sb.append(data[0]);
                for (int i = 1; i < data.length; i++) {
                    sb.append(",");
                    sb.append(data[i]);
                }
                sb.append("\n");


            }


        }
        FileWriter csvWriter = new FileWriter("src/main/resources/metadata.csv");

        csvWriter.append(sb);
        csvWriter.close();
        csvReader.close();

        for (int k = 0; k < columnNames.length; k++) {
            if (!colNames.contains(columnNames[k]))
                throw new DBAppException("The table does not contain this column: " + columnNames[k]);
        }

        GridIndex GI = new GridIndex(tableName, columnNames, (t.getGridIndices().size()));
        t.rehomeAlreadyMadeRows(GI);
        serialize(GI, tableName + "-GI" + (t.getGridIndices().size()));
        Vector<GridIndex> indices = t.getGridIndices();
        indices.add(GI);
        t.setGridIndices(indices);
        t.setHasGrid(true);
        t.getGridIndices_colNames().add(columnNames);
        t.setGridIndices_colNames(t.getGridIndices_colNames());
        serialize(t, tableName);


    }

    @Override
    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException { //OR AND XOR


        if (sqlTerms.length == 0) throw new DBAppException("There are no sql terms to search for");
        if (arrayOperators.length != sqlTerms.length - 1)
            throw new DBAppException("Number of operators should be equal to number of SQL Terms minus 1");

        SQLTerm sq = sqlTerms[0];
        String tablename = sq._strTableName;
        String colname = sq._strColumnName;
        String operator = sq._strOperator;
        Object value = sq._objValue;

        Table t = (Table) deserialize(tablename);

        Vector<Vector<Object>> returnedrows = new Vector<Vector<Object>>();

        Hashtable htVal = new Hashtable<>();
        htVal.put(colname, value);
        Hashtable htOp = new Hashtable<>();
        htOp.put(colname, operator);

        returnedrows = t.selectfromTable(htVal, htOp);

        int index = 1;
        while (index != sqlTerms.length) {
            htVal = new Hashtable<>();
            htOp = new Hashtable<>();
            htVal.put(sqlTerms[index]._strColumnName, sqlTerms[index]._objValue);
            htOp.put(sqlTerms[index]._strColumnName, sqlTerms[index]._strOperator);

            switch (arrayOperators[index - 1]) {
                case "AND":
                    returnedrows = AND(returnedrows, t.selectfromTable(htVal, htOp));
                    break;

                case "OR":
                    returnedrows = OR(returnedrows, t.selectfromTable(htVal, htOp));
                    break;

                case "XOR":
                    returnedrows = XOR(returnedrows, t.selectfromTable(htVal, htOp));
                    break;
            }
            index++;
        }

        Vector<String> v = new Vector<String>();

        Iterator<Vector<Object>> Itreturned = returnedrows.iterator();
        for (int i = 0; i < returnedrows.size(); i++) {
            v.add(returnedrows.get(i).toString());
        }
        Iterator<String> IS = v.iterator();

        serialize(t, tablename);
        return IS;
    }



    /*HELPER METHODS*/

    private Vector<Vector<Object>> XOR(Vector<Vector<Object>> returnedrows, Vector<Vector<Object>> selectfromTable) {

        Iterator<Vector<Object>> i1 = returnedrows.iterator();
        Iterator<Vector<Object>> i2 = selectfromTable.iterator();
        Vector<Vector<Object>> res = new Vector<Vector<Object>>();

        while (i1.hasNext()) {
            Vector<Object> row1 = (Vector<Object>) i1.next();
            if (!(selectfromTable.contains(row1))) {
                res.add(row1);
            }

        }

        while (i2.hasNext()) {
            Vector<Object> row2 = (Vector<Object>) i2.next();
            if (!(returnedrows.contains(row2))) {
                res.add(row2);
            }
        }

        return res;
    }

    private Vector<Vector<Object>> OR(Vector<Vector<Object>> returnedrows, Vector<Vector<Object>> selectfromTable) { //present in either or both

        Iterator<Vector<Object>> i1 = returnedrows.iterator();
        Iterator<Vector<Object>> i2 = selectfromTable.iterator();
        Vector<Vector<Object>> res = new Vector<Vector<Object>>();

        while (i1.hasNext()) {
            Vector<Object> row1 = (Vector<Object>) i1.next();
            res.add(row1);

        }
        while (i2.hasNext()) {
            Vector<Object> row1 = (Vector<Object>) i2.next();
            if (!res.contains(row1))
                res.add(row1);

        }

//        while(i1.hasNext()){
//
//            Vector<Object> row1 =(Vector<Object>) i1.next();
//            while(i2.hasNext()){
//                Vector<Object> row2 =(Vector<Object>) i2.next();
//                if(row1.equals(row2)){
//                    res.add(row1);
//                    break;
//                }else{
//                    res.add(row2);
//                    if(!(res.contains(row1)))
//                        res.add(row1);
//                }
//            }
//
//        }
        return res;
    }

    private Vector<Vector<Object>> AND(Vector<Vector<Object>> returnedrows, Vector<Vector<Object>> selectfromTable) { //values in both
        Iterator<Vector<Object>> i1 = returnedrows.iterator();
        Iterator<Vector<Object>> i2 = selectfromTable.iterator();
        Vector<Vector<Object>> res = new Vector<Vector<Object>>();
        while (i1.hasNext()) {
            Vector<Object> row1 = (Vector<Object>) i1.next();
            while (i2.hasNext()) {
                Vector<Object> row2 = (Vector<Object>) i2.next();

                if (row1.equals(row2)) {
                    res.add(row1);
                    break;
                }
            }

        }
        return res;
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

    public ArrayList<String> getColNamesOfTable(String tableName) throws DBAppException, IOException {
        String csvLine;
        // names of columns with the order
        ArrayList<String> colNames = new ArrayList<>();
        BufferedReader csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
        int pk_found = -1;
        boolean found = false;
        int index = 0;
        while ((csvLine = csvReader.readLine()) != null) {

            String[] data = csvLine.split(",");
            if (data[0].equals(tableName)) {
                found = true;
                colNames.add(data[1]);


                if (data[3].equals("True") || data[3].equals("true")) {
                    pk_found = index;
                }
                index++;

            } else if (!data[0].equals(tableName) && found == true)
                break;

        }
        csvReader.close();
        return colNames;
    }

    private String checkColumnTypes(Hashtable<String, String> colNameType) //check if user entered correct type while creating table
    {
        HashSet<String> dataTypes = new HashSet<String>();
        dataTypes.add("java.lang.Integer");
        dataTypes.add("java.lang.String");
        dataTypes.add("java.util.Date");
        dataTypes.add("java.lang.Double");


        for (Entry<String, String> entry : colNameType.entrySet())
            if (!dataTypes.contains(entry.getValue()))
                return entry.getKey();
        return null;
    }

    public int getPropValues(String string) throws IOException {
        Properties prop = new Properties();
        FileInputStream inputStream = new FileInputStream("src/main/resources/DBApp.config");
        prop.load(inputStream);

        return Integer.parseInt(prop.getProperty(string));
    }

    public static Object parse(String keytype, String strClusteringKey) {
        if (keytype.equals(types[0]))  // Integer
        {
            return Integer.parseInt(strClusteringKey);
        } else if (keytype.equals(types[1])) // String
        {
            return strClusteringKey;
        } else if (keytype.equals(types[2]))  // Double
        {
            return Double.parseDouble(strClusteringKey);
        } else // Date
        {
            return parseDate(strClusteringKey);
        }

    }

    public static Date parseDate(String s) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

        Date date = null;
        try {
            date = format.parse(s);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date;
    }

    public static long getdifferencedate(String d1, String d2) {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDate date1 = LocalDate.parse(d1, dtf);
        LocalDate date22 = LocalDate.parse(d2, dtf);
        long daysBetween = ChronoUnit.DAYS.between(date1, (Temporal) date22);
        return daysBetween;
    }

    public static String getLD(String d) {
        String input = d + "";
        DateTimeFormatter f = DateTimeFormatter.ofPattern("E MMM dd HH:mm:ss z uuuu").withLocale(Locale.US);

        ZonedDateTime zdt = ZonedDateTime.parse(input, f);
        LocalDate ld = zdt.toLocalDate();
        return ld + "";

    }

    public static void serialize(Object e, String fileName) {
        try {
            FileOutputStream fileOut =
                    new FileOutputStream("src/main/resources/data/" + fileName + ".class");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(e);
            out.close();
            fileOut.close();
            System.out.println("Serialized data is saved in " + fileName + ".class");

        } catch (IOException i) {
            i.printStackTrace();
        }

    }

    public static Object deserialize(String fileName) {
        try {
            FileInputStream fileIn = new FileInputStream("src/main/resources/data/" + fileName + ".class");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            Object e = in.readObject();
            in.close();
            fileIn.close();
            System.out.println("deserialized data from " + fileName + ".class");

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


}
