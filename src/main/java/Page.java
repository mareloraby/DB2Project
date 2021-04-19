
import java.util.Hashtable;
import java.util.Vector;

public class Page implements java.io.Serializable  {

    public Vector<Hashtable> page;
    public Hashtable row;

    public Page(Hashtable s){
        this.row = s;
        this.page = new Vector<Hashtable>();
        this.page.add(this.row);
    }

    public static void main(String[] args) {
        // write your code here

    }
}
