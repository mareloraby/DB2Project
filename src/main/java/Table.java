import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Vector;

public class Table implements java.io.Serializable  {
//int numOfPages;
private String tableName;
private Vector<Page> pages; // number of pages-- pages.size() and pages id-- page.get();
private int sizeOfLastPage;
public Table(String name){
    pages= new Vector<Page>();
    tableName= name;

}
public void addPage(){
Page p = new Page();
pages.add(p);
}
}
