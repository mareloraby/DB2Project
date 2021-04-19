
import java.util.Hashtable;
import java.util.Vector;

public class Page implements java.io.Serializable  {
    private Vector<Hashtable> rows;
//    public Vector<Hashtable> page;
//    public Hashtable row;

    public Page(){
//        this.row = s;
        rows = new Vector<Hashtable>();
//        this.page.add(this.row);
    }

    public void addRow(Hashtable s){
        rows.add(s);
    }
    public static void main(String[] args) {
        // write your code here

    }
}
