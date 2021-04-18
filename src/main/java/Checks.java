import java.io.IOException;
import java.util.Hashtable;

public class Checks{
    public static void main(String[] args) throws DBAppException, IOException {
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
        dbApp.createTable( strTableName, "id", htblColNameType , min , max );

    }

}
