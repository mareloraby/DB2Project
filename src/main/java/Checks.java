import java.io.IOException;
import java.util.Hashtable;

public class Checks{
    public static void main(String[] args) throws DBAppException {
/*expections:
clusteringKey not null
table name doesn't exist
all col names and types are entered //each colname has a type as well as max and min values
 */
        String strTableName = "Student";
        DBApp dbApp = new DBApp( );
        Hashtable htblColNameType = new Hashtable( );
        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.double");
        Hashtable min = new Hashtable();
        min.put("id","0");
        min.put("name","A");
        min.put("gpa","0");
        Hashtable max = new Hashtable();
        max.put("id","10000");
        max.put("name","ZZZZZZZZZZZ");
        max.put("gpa","1000000");
        try {
            dbApp.createTable( strTableName, "id", htblColNameType , min , max );
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
