
import java.util.Hashtable;
import java.util.Vector;

public class Page implements java.io.Serializable  {
    private Vector<Hashtable> rows;
    private int min_Pk_Value;
    private int max_Pk_Value;
    private int numOfRows;

//    public Vector<Hashtable> page;
//    public Hashtable row;

    public Page(){
        min_Pk_Value=0;
        max_Pk_Value=0;
        rows = new Vector<Hashtable>();
//this.page.add(this.row);
    }

    public void addRow(Hashtable s){
        numOfRows++; rows.add(s);
    }
    public static void main(String[] args) {
        // write your code here

    }
}
