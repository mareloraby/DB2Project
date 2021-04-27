import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Vector;

public class Table implements java.io.Serializable {
    //int numOfPages;
    private static final long serialVersionUID = 1L;
    private String tableName;
    private int count;
    transient private Vector<Vector<Object>> pagesInfo;
    transient private Vector<Object> pks;
    transient private Vector<Integer> pagesID; // number of pages-- pages.size() and pages id-- page.get();
    private int maxRows;

    //index: 1,2
    //names: 1,3

    //transient private Vector<Vector<Object>> min_max_count;
    public String getTableName() {
        return tableName;
    }

    public Table(String name) {
        maxRows = DBApp.MaximumRowsCountinPage;
        //min_max_count= new Vector<Vector<Object>>();
        pagesID = new Vector<Integer>();
        tableName = name;
        count = 0;
    }

    // name of pages: tableName/pageID/path

    public int getCount() {
        return count;
    }
//100-200, 300-400 (both are full and I want to insert 250)

    public void insertIntoPage(Vector<Object> v, int index) throws DBAppException {

        // check if the page is full, if yes, go to the next page and check whether it has space
        // if the next page has space, insert into the second page and do the needed shifting
        // otherwise create an overflow in the initial page we wanted to insert in
        // update min/max if needed
        // insert into the right page then call sortI

        // get the clusteringKey ck of v
        Object pk = v.get(index);

        // check whether the table has pages
        // if there is no page, create a new one and insert
        // insert the info related to this page into the pagesInfo Vector
        if (count == 0) {
            addPage(v, index);
        }


        // check if pk already exists in the table
        if (pks.contains(pk))
            throw new DBAppException("You can not insert a duplicate key for a primary key.");
        else {
            pks.add(pk);
        }

// 0-100(has no space) , 120-130(has space)
// I want to insert 30, but 30 should not go into 120-130!, thus I have to insert in the prev page(0-100)
// which means I need to shift one record from 0-100 to 120-130 and insert into 0-100
// however if 120-130 is also full, create an overflow page for 0-100


        // check whether the pk fits within the ranges of min-max of any existing page
        for (int i = 0; i < (pagesID).size(); i++) {
            // retrieve the info of the page we're standing at
            Vector<Object> page = pagesInfo.get(i);
            int countRows = (int) page.get(0); // <"2,0,10000", >
            Object min = page.get(1);
            Object max = page.get(2);
//            int countRows = p.getNumOfRows(); // <"2,0,10000", >
//            Object min = p.getMin_pk_value();
//            Object max = p.getMax_pk_value();

            // if we are on the last page in the table
            if (i == (pagesID.size() - 1)) {
                // check whether we have room for a new row in the last page.If so, add the new row.
                if (countRows < maxRows) {
                    Page p = (Page) DBApp.deserialize(tableName + "/" + pagesID.get(i));
                    // after adding the new row, update the page info and add it into the pagesInfo Vector in the table.
                    Vector<Object> updatePage = p.addRow(v, index);
                    pagesInfo.remove(i);
                    pagesInfo.add(i, updatePage);
                    // sort in the vector
                    p.sortI(index);
                    DBApp.serialize(p, tableName + "/" + pagesID.get(i));
                    break;
                } else {
                    // if pk is greater than max, add new page ( no overflow pages for the last page)
                    if (Page.compare(pk, max) == 1) {
                        addPage(v, index);
                    }
                    // if pk is less than the max in the last page, create a new page
                    // and add the last row in the last page to the new page
                    // then insert into the last page ( not the new one)
                    else {
                        Page p = (Page) DBApp.deserialize(tableName + "/" + pagesID.get(i));
                        Vector<Vector> rows_in_prevPage = p.getRows();
                        Vector<Object> to_be_shifted = rows_in_prevPage.get(p.getNumOfRows() - 1);
                        p.setRows(p.getRows().remove(p.getNumOfRows() - 1));
                        Vector<Object> updatePage = p.addRow(v, index);
                        pagesInfo.remove(i);
                        pagesInfo.add(i, updatePage);
                        p.sortI(index);
                        addPage(to_be_shifted, index);
                        DBApp.serialize(p, tableName + "/" + pagesID.get(i));
                        break;
                    }
                }

            }


            // check if the required page has an overflow page,
            // if so, insert into the overflow page if this overflow page has space,
            // otherwise create a new overflow page
            // if (Page.compare(max, pk) == 1 ) {
            if (Page.compare(max, pk) == 1) {
                Page p = (Page) DBApp.deserialize(tableName + "/" + pagesID.get(i));
                if (countRows == maxRows) {
                    if (p.getOverFlow() != null) {
                        int overflowID = 1;
                        // check for the first overflow page that has room for a new record
                        while (true) {
                            Page o = (Page) DBApp.deserialize(tableName + "/" + pagesID.get(i) + "." + overflowID);
                            if (o.getNumOfRows() < o.getMaxRows()) {
                                o.addRow(v, index);
                                o.sortI(index);
                                p.setOverFlow(o);
                                DBApp.serialize(o, tableName + "/" + count + "." + ((overflowID) + ""));
                                break;
                            }
                            // if the overflow page we are at is full,
                            // check if there is another overflow page linked to it
                            // if not, create a new overflow page and insert the new row in it
                            else {
                                if (o.getOverFlow() != null)
                                    overflowID++;
                                else {
                                    Page new_overflow = new Page();
                                    new_overflow.addRow(v, index);
                                    o.setOverFlow(new_overflow);
                                    DBApp.serialize(new_overflow, tableName + "/" + count + "." + ((overflowID + 1)));
                                    break;
                                }
                            }
                        }
                        break;
                    }
                    /* if the main page we wanted to insert the new row in is full and does not have an overflow page,
                    check the following page. If the following page has room, insert in the following page, otherwise,
                    create a new overflow page linked to the main page. */
                    else if ((i + 1) < pagesID.size()) {
                        Vector<Object> page2 = pagesInfo.get(i);
                        int countRows2 = (int) page2.get(0); // <"2,0,10000", >
                        Object min2 = page2.get(1);
                        Object max2 = page2.get(2);
//                        Page page2 = (Page) DBApp.deserialize(tableName + "/" + pagesID.get(i + 1))
//                        int countRows2 = p2.getNumOfRows();
//                        Object min2 = p2.getMin_pk_value();
//                        Object max2 = p2.getMax_pk_value();
                        // checking for overflow
                        if (countRows2 < maxRows) {
                            Page p2 = (Page) DBApp.deserialize(tableName + "/" + pagesID.get(i + 1));
                            Vector<Vector> rows_in_prevPage = p.getRows();
                            Vector<Object> to_be_shifted = rows_in_prevPage.get(p.getNumOfRows() - 1);
                            p2.addRow(to_be_shifted, index);
                            p.setRows(p.getRows().remove(p.getNumOfRows() - 1));
                            Vector<Object> updatePage = p.addRow(v, index);
                            pagesInfo.remove(i);
                            pagesInfo.add(i, updatePage);
                            p.sortI(index);
                            DBApp.serialize(p2, tableName + "/" + pagesID.get(i + 1));
                            DBApp.serialize(p, tableName + "/" + pagesID.get(i));
                            break;
                        } else {
                            //create an overflow page and insert into the new page
                            Page o = new Page();
                            o.addRow(v, index);
                            p.setOverFlow(o);
                            DBApp.serialize(o, tableName + "/" + count + "." + 1);
                            break;
                        }
                    }
                } else {
                    // the pk lies within the range and there is room for it, so we add immediately into the page
                    Vector<Object> updatePage = p.addRow(v, index);
                    pagesInfo.remove(i);
                    pagesInfo.add(i, updatePage);
                    p.sortI(index);
                    DBApp.serialize(p, tableName + "/" + pagesID.get(i));
                    break;
                }
            } else if (countRows < maxRows) {
                // pk greater than the max but there is room in the page.
                Page p = (Page) DBApp.deserialize(tableName + "/" + pagesID.get(i));
                Vector<Object> updatePage = p.addRow(v, index);
                pagesInfo.remove(i);
                pagesInfo.add(i, updatePage);
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
        Vector<Object> newPage = new Vector<Object>();
        newPage.add(1);
        newPage.add(v.get(index));
        newPage.add(v.get(index));
        pagesInfo.add(newPage);
        DBApp.serialize(p, tableName + "/" + count);
    }

    public int binarySearch(Vector<Vector<Object>> arr, int l, int r, Object x) {
        if (r >= l) {
            int mid = l + (r - l) / 2;

            // If the element is present at the
            // middle itself
            int arr_mid= (int)(arr.get(mid)).get(2);
            if (Trial.compare(arr_mid,x)==0)
                return mid;

            // If element is smaller than mid, then
            // it can only be present in left subarray
            if (Trial.compare(arr_mid,x)==1)
                return binarySearch(arr, l, mid - 1, x);

            // Else the element can only be present
            // in right subarray
            return binarySearch(arr, mid + 1, r, x);
        }

        // We reach here when element is not present
        // in array
        return -1;
    }

    public void deleteFromPage(Vector<Vector> index_value, int pk_found, Object pk_value) {

        if (pk_value != null) {
           int searchPage= binarySearch(pagesInfo,0, pagesInfo.size(), pk_value);
           Page p = (Page) DBApp.deserialize(tableName + "/" + pagesID.get(searchPage));
            // page min w max

            // linear search 3la el pages ---

            // 200
            // 0-100 , 101-300, 350-400
            // 100,300,400

        }
    }
}
