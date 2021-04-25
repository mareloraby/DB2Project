import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Vector;

public class Table implements java.io.Serializable {
    //int numOfPages;
    private static final long serialVersionUID = 1L;
    private String tableName;
    private int count;

    transient private Vector<Object> pks;
    transient private Vector<Integer> pagesID; // number of pages-- pages.size() and pages id-- page.get();

    //transient private Vector<Vector<Object>> min_max_count;
    public String getTableName() {
        return tableName;
    }

    public Table(String name) {
        //min_max_count= new Vector<Vector<Object>>();
        pagesID = new Vector<Integer>();
        tableName = name;
        count = 0;
    }


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
        if (count == 0)
            addPage(v, index);

// get the clusteringKey ck of v
        Object pk = v.get(index);

        if(pks.contains(pk))
            throw new DBAppException("You can not insert a duplicate key for a primary key.");
        else {
            pks.add(pk);
     }

// 0-100(has no space) , 120-130(has space)
// I want to insert 30, but 30 should not go into 120-130!, thus I have to insert in the prev page(0-100)
// which means I need to shift one record from 0-100 to 120-130 and insert into 0-100
// however if 120-130 is also full, create an overflow page for 0-100

// check whether the pk fits within the ranges of min-max of any existing page
        //  Table t= (Table) DBApp.deserialize("T1");
        for (int i = 0; i < (pagesID).size(); i++) {
            Page p = (Page) DBApp.deserialize(tableName + "/" + pagesID.get(i));
            int countRows = p.getNumOfRows();
            Object min = p.getMin_pk_value();
            Object max = p.getMax_pk_value();
            if (i == (pagesID.size() - 1)) {
                if (countRows < p.getMaxRows()) {
                    p.addRow(v, index);
                    p.sortI(index);
                    DBApp.serialize(p, tableName + "/" + pagesID.get(i));
                    break;
                } else {

                    if (p.compare(pk, max) == 1) {
                        addPage(v, index);
                    } else {
                        Vector<Vector> rows_in_prevPage = p.getRows();
                        Vector<Object> to_be_shifted = rows_in_prevPage.get(p.getNumOfRows() - 1);
                        p.setRows(p.getRows().remove(p.getNumOfRows() - 1));
                        p.addRow(v, index);
                        p.sortI(index);
                        addPage(to_be_shifted, index);
                        DBApp.serialize(p, tableName + "/" + pagesID.get(i));
                        break;
                    }
                }

            }
            // check if this page has an overflow page, if so, insert into the overflow page
            // if this overflow page has space, otherwise create a new overflow page
            if (p.compare(pk, min) == 1 && p.compare(max, pk) == 1) {
                if (countRows == p.getMaxRows()) {
                    if (p.getOverFlow() != null) {
                        char overflowID = 'a';
                        while (true) {
                            Page o = (Page) DBApp.deserialize(tableName + "/" + pagesID.get(i) + (overflowID + ""));
                            if (o.getNumOfRows() < o.getMaxRows()) {
                                o.addRow(v, index);
                                o.sortI(index);
                                p.setOverFlow(o);
                                DBApp.serialize(o, tableName + "/" + count + ((overflowID) + ""));
                                break;
                            } else {
                                if (o.getOverFlow() != null)
                                    overflowID++;
                                else {
                                    Page new_overflow = new Page();
                                    new_overflow.addRow(v, index);
                                    o.setOverFlow(new_overflow);
                                    DBApp.serialize(new_overflow, tableName + "/" + count + ((overflowID + 1) + ""));
                                    break;
                                }
                            }
                        }
                        break;
                    } else if ((i + 1) < pagesID.size()) {
                        Page p2 = (Page) DBApp.deserialize(tableName + "/" + pagesID.get(i + 1));
                        int countRows2 = p2.getNumOfRows();
                        Object min2 = p2.getMin_pk_value();
                        Object max2 = p2.getMax_pk_value();
                        // checking for overflow
                        if (countRows2 < p2.getMaxRows()) {
                            Vector<Vector> rows_in_prevPage = p.getRows();
                            Vector<Object> to_be_shifted = rows_in_prevPage.get(p.getNumOfRows() - 1);
                            p2.addRow(to_be_shifted, index);
                            p.setRows(p.getRows().remove(p.getNumOfRows() - 1));
                            p.addRow(v, index);
                            p.sortI(index);
                            DBApp.serialize(p2, tableName + "/" + pagesID.get(i + 1));
                            DBApp.serialize(p, tableName + "/" + pagesID.get(i));
                            break;
                        } else {
                            //create an overflow page and insert into the new page
                            Page o = new Page();
                            o.addRow(v, index);
                            p.setOverFlow(o);
                            DBApp.serialize(o, tableName + "/" + count + "a");
                            break;
                        }
                    }
                } else {
                    // the pk lies within the range, so we add immediately into the page
                    p.addRow(v, index);
                    p.sortI(index);
                    DBApp.serialize(p, tableName + "/" + pagesID.get(i));
                    break;
                }
            } else if (countRows < p.getMaxRows()) {
                p.addRow(v, index);
                p.sortI(index);
                DBApp.serialize(p, tableName + "/" + pagesID.get(i));
                break;
            }
        }
    }


    public void addPage(Vector<Object> v, int index) throws DBAppException {
        count++;
        Page p = new Page();
        p.addRow(v, index);
        pagesID.add(count);
        DBApp.serialize(p, tableName + "/" + count);
    }

}
