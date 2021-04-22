
import java.util.Hashtable;
import java.util.Vector;

public class Page implements java.io.Serializable  {
    private Vector<Vector> rows;
    private Object min_Pk_Value;
    private Object max_Pk_Value;
    private int numOfRows;
    private int maxRows;

    public Page(){
        maxRows=200;
        rows = new Vector<Vector>();
    }

    public void addRow(Vector s) throws DBAppException {
       if(numOfRows==maxRows) {
           throw new DBAppException("The page is full!");
       }
        numOfRows++; rows.add(s);
    }
    public static void main(String[] args) {
        // write your code here

    }
}
