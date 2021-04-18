import java.io.FileWriter;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;

public class DBApp implements DBAppInterface{

    @Override
    public void init() throws IOException {
//Hi rawan // Hi Maryam, hru? // hi mariam // Hi Hadeer :)
    }

    @Override
    public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType, Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException, IOException {
        FileWriter csvWriter = new FileWriter("metadata.csv", true);


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
            csvWriter.append(colname);
            csvWriter.append(",");
            csvWriter.append(coltype);
            csvWriter.append(",");
               if(clusteringKey.equals(colname))
                  csvWriter.append("True");
               else
                   csvWriter.append("False");
            csvWriter.append(",");
            csvWriter.append("False");
            csvWriter.append(",");

            String min = keysmin.nextElement();
            String minvalue = colNameMin.get(min);
            csvWriter.append(minvalue);
            csvWriter.append(",");

            String max = keysmax.nextElement();
            String maxvalue = colNameMax.get(max);
            csvWriter.append(maxvalue);
            csvWriter.append("\n");

        }
        csvWriter.close();

    }

    @Override
    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {

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

}
