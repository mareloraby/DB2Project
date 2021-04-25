import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Vector;

public class Table implements java.io.Serializable  {
//int numOfPages;
private static final long serialVersionUID = 1L;
private String tableName;
private int count;

transient private Vector<Integer> pagesID; // number of pages-- pages.size() and pages id-- page.get();
//transient private Vector<Vector<Object>> min_max_count;
    public String getTableName() {
        return tableName;
    }

    public Table(String name){
//        min_max_count= new Vector<Vector<Object>>();
    pagesID= new Vector<Integer>();
    tableName= name;
    count=0; }


// name of pages: tableName/pageID/path

    public int getCount() {
        return count;
    }

    public void insertIntoPage(Vector<Object> v, int index) throws DBAppException {

    // check if the page is full, if yes, go to the next page and check whether it has space
    // if the next page has space, insert into the second page and do the needed shifting
    // otherwise create an overflow in the initial page we wanted to insert in
    // update min/max if needed
    // insert into the right page then call sortI

// check whether the table has pages // if there is no page, create a new one and insert
    if(count==0)
      addPage(v, index);

// get the clusteringKey ck of v
        Object pk= v.get(index);

// 0-100(has no space) , 120-130(has space)
// I want to insert 30, but 30 should not go into 120-130!, thus I have to insert in the prev page(0-100)
// which means I need to shift one record from 0-100 to 120-130 and insert into 0-100
// however if 120-130 is also full, create an overflow page for 0-100

// check whether the pk fits within the ranges of min-max of any existing page
      //  Table t= (Table) DBApp.deserialize("T1");
        for(int i=0; i<(pagesID).size(); i++){
            Page p= (Page) DBApp.deserialize(tableName+"/"+ pagesID.get(i));
            int countRows= p.getNumOfRows();
            Object min= p.getMin_pk_value();
            Object max= p.getMax_pk_value();
            // if it is full
                if(Trial.compare(pk, min)==1 && Trial.compare(max,pk)==1){
                    if(countRows==p.getMaxRows()){
                        // check if this page has an overflow page, if so, insert into the overflow page
                        // if this overflow page has space, otherwise create a new overflow page

                        // does the page have a VECTOR of overflow pages? or should we just show that it is linked to another page?
if((i+1)<pagesID.size()){
    Page p2= (Page) DBApp.deserialize(tableName+"/"+ pagesID.get(i+1));
    int countRows2= p2.getNumOfRows();
    Object min2= p2.getMin_pk_value();
    Object max2= p2.getMax_pk_value();
    // checking for overflow
    if(countRows2<p2.getMaxRows()) {
        Vector<Vector> rows_in_prevPage= p.getRows();
        Vector<Object> to_be_shifted= rows_in_prevPage.get(p.getNumOfRows()-1);
        p2.addRow(to_be_shifted, index);

        p.setRows(p.getRows().remove(p.getNumOfRows()-1));
        p.addRow(v, index);
        p.sortI(index);
    }
    else{
// create an overflow page and insert into the new page
    }
}
                    }
                    else {
                        p.addRow(v, index);
                        p.sortI(index);}
                }
                else if(countRows<p.getMaxRows()) {
                    p.addRow(v, index);
                    p.sortI(index);
                }
            }
        }





public void addPage(Vector<Object> v, int index) throws DBAppException {
        count++;
Page p = new Page();
//pages.add(p);
p.addRow(v, index);
pagesID.add(count);
//Vector<Object> mmc= new Vector<Object>();
//mmc.add(<0,0,1>)
//min_max_count.add()
DBApp.serialize(p, tableName+"/"+count);
}

}
