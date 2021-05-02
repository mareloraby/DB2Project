import java.io.FileNotFoundException;
import java.util.Date;
import java.util.Enumeration;
import java.util.Vector;

public class Table implements java.io.Serializable {

    private static final long serialVersionUID = 1L;
    private String tableName;
    private int count;
    private Vector<Vector<Object>> pagesInfo;
    private Vector<Object> pks;
    private Vector<Integer> pagesID; // number of pages-- pages.size() and pages id-- page.get();
    private int maxRows;

    // page p-> overflowID vector:<1,2,3>
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
        pagesInfo = new Vector<Vector<Object>>();
        pks = new Vector<Object>();
        tableName = name;
        count = 0;
    }

    // name of pages: tableName/pageID/path

    public int getCount() {
        return count;
    }

    //100-200, 300-400 (both are full and I want to insert 250)
    public Vector<Vector<Object>> updatePageOverflowInfo(Page p, Page o, int j) {
        Vector<Vector<Object>> oldOverflowInfoPages = p.getOverFlowInfo();
        Vector<Object> oldOverflow = oldOverflowInfoPages.get(j);
        oldOverflow.setElementAt(o.getNumOfRows(), 1);
        oldOverflowInfoPages.setElementAt(oldOverflow, j);
        return oldOverflowInfoPages;
    }

    public void checktablePage() {
        for (int i = 0; i < (pagesID).size(); i++) {
            Vector<Object> page = pagesInfo.get(i);

            Enumeration enu = pagesInfo.get(i).elements();
            System.out.println("Pages " + pagesID.get(i) + " " + "in " + tableName + " with pagesCount " + pagesInfo.get(i).get(0));
            // Displaying the Enumeration
            while (enu.hasMoreElements()) {
                System.out.println(enu.nextElement());
            }
        }
    }

    public void printPages() {
        for (int i = 0; i < (pagesID).size(); i++) {
            Vector<Object> page = pagesInfo.get(i);
            Page p = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(i));
            Vector<Vector<Object>> r = p.getRows();
            System.out.println("Page " + pagesID.get(i) + " " + "in " + tableName + " with pagesCount " + pagesInfo.get(i).get(0));
            // rows of page:
            for (int j = 0; j < r.size(); j++) {
                Enumeration enu = r.get(j).elements();
//                // Displaying the Enumeration
                while (enu.hasMoreElements()) {
                    System.out.println(enu.nextElement() + " ");
                }
            }
            System.out.println();
            // check if this page has overflows:
            if (p.getOverFlowInfo() != null) {
                Vector<Vector<Object>> overflowPagesInfo = p.getOverFlowInfo();
                for (int k = 0; k < p.getOverFlowInfo().size(); k++) {
                    Vector<Object> overflow = overflowPagesInfo.get(k);
                    int ID = (int) overflow.get(0);
                    Page o = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(i) + "." + ID);
                    Vector<Vector<Object>> rowsOverflow = o.getRows();
                    System.out.println("PageOVERFLOW" + pagesID.get(i) + " " + ID + " " + "in " + tableName + " with pagesCount " + overflow.get(1));
                    for (int l = 0; l < o.getRows().size(); l++) {
                        Enumeration enu1 = rowsOverflow.get(k).elements();

                        // Displaying the Enumeration
                        while (enu1.hasMoreElements()) {
                            System.out.println(enu1.nextElement());

                        }
                    }
                }
            }
        }
    }

    public void insertIntoPage(Vector<Object> v, int index) throws DBAppException {
        checktablePage();

        // check if the page is full, if yes, go to the next page and check whether it has space
        // if the next page has space, insert into the second page and do the needed shifting
        // otherwise create an overflow in the initial page we wanted to insert in
        // update min/max if needed
        // insert into the right page then call sortI

        // get the clusteringKey ck of v
        Object pk = v.get(index);
        System.out.println("THE VALUE TO BE INSERTED IS " + pk);
        // check whether the table has pages
        // if there is no page, create a new one and insert
        // insert the info related to this page into the pagesInfo Vector
        if (count == 0) {
            addPage(v, index);

            if (pks.contains(pk))
                throw new DBAppException("You can not insert a duplicate key for a primary key.");
            else {
                pks.add(pk);
            }

        } else {

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

            int searchPage = binarySearch(pagesInfo, 0, pagesInfo.size() - 1, pk);
            // check whether the pk fits within the ranges of min-max of any existing page
//            for (int i = 0; i < (pagesID).size(); i++) {
            int i = searchPage;
            while (true) {
                System.out.println(i);
                if (i == -1) {
                    System.out.println("ur i is -1 ");
                    i = pagesID.size() - 1;
                }
                System.out.println("the amazing searchKey " + i);
                // retrieve the info of the page we're standing at
                Vector<Object> page = pagesInfo.get(i);

                Enumeration enu = pagesInfo.get(i).elements();
                System.out.println("The enumeration of values are:");
                // Displaying the Enumeration
                while (enu.hasMoreElements()) {
                    System.out.println(enu.nextElement());
                }

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
                        Page p = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(i));
                        // after adding the new row, update the page info and add it into the pagesInfo Vector in the table.
                        Vector<Object> updatePage = p.addRow(v, index);
                        pagesInfo.remove(i);
                        pagesInfo.add(i, updatePage);
                        System.out.println("inserted here!!!" + 1 + " " + pagesID.get(i) + " " + "and page count is" + " " + countRows);

                        // sort in the vector
                        p.sortI(index);
                        DBApp.serialize(p, tableName + "-" + pagesID.get(i));
                        break;
                        // In case we want to refer back to binary search in the page itself:
//                    pagesInfo.remove(i);
//                    // sort in the vector
//                    p.sortB(pk,v);
//                    Vector<Object> pInfo= new Vector<Object>();
//                    pInfo.add(p.getNumOfRows());
//                    pInfo.add(p.getMin_pk_value());
//                    pInfo.add(p.getMax_pk_value());
//                    pagesInfo.add(i, pInfo);
                    } else {
                        // if pk is greater than max, add new page ( no overflow pages for the last page)
                        if (Page.compare(pk, max) > 0) {
                            addPage(v, index);
                            break;
                        }
                        // if pk is less than the max in the last page, create a new page
                        // and add the last row in the last page to the new page
                        // then insert into the last page ( not the new one)
                        else {
                            Page p = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(i));
                            Vector<Vector<Object>> rows_in_prevPage = p.getRows();
                            Vector<Object> to_be_shifted = rows_in_prevPage.get(p.getNumOfRows() - 1);
                            Vector<Vector<Object>> r = p.getRows();
                            r.remove(p.getNumOfRows() - 1);
                            p.setRows(r);
                            Vector<Object> updatePage = p.addRow(v, index);
                            pagesInfo.remove(i);
                            pagesInfo.add(i, updatePage);
                            System.out.println("inserted here!!!" + 2 + " " + pagesID.get(i) + "and page count is" + " " + countRows);
                            p.sortI(index);
                            addPage(to_be_shifted, index);
                            for (int k = 0; k < pagesID.size(); k++) {
                                System.out.print("All the pages available" + " " + pagesID.get(k));
                                System.out.println();
                            }

                            DBApp.serialize(p, tableName + "-" + pagesID.get(i));
                            break;
                        }
                    }

                }


                // check if the required page has an overflow page,
                // if so, insert into the overflow page if this overflow page has space,
                // otherwise create a new overflow page
                // if (Page.compare(max, pk) == 1 ) {
                if (Page.compare(max, pk) >= 0) {
                    System.out.println(max + " " + pk);
                    Page p = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(i));


                    if (countRows > maxRows) {
                        if (p.getOverFlowInfo().size() != 0) {
                            boolean found_space = false;
                            Vector<Vector<Object>> overflowPagesInfo = p.getOverFlowInfo();
                            for (int j = 0; j < p.getOverFlowInfo().size(); j++) {
                                Vector<Object> overflow = overflowPagesInfo.get(j);
                                int ID = (int) overflow.get(0);
                                int numOfRowsInOverflow = (int) overflow.get(1);
                                Page o = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(i) + "." + ID);
                                if (o.getNumOfRows() < maxRows) {
                                    o.addOverflowRow(v);
                                    o.sortI(index);

                                    if (ID == 2) System.out.println("OVERFLOW PAGE");
                                    System.out.println("inserted here!!!" + 4 + " " + pagesID.get(i) + "and page count is" + " " + countRows);
                                    Vector<Vector<Object>> updatedOverflowInfoPages = updatePageOverflowInfo(p, o, j);
                                    p.setOverFlowInfo(updatedOverflowInfoPages);
                                    DBApp.serialize(o, tableName + "-" + pagesID.get(i) + "." + (ID + ""));
                                    found_space = true;
                                    break;
                                }
                                DBApp.serialize(o, tableName + "-" + pagesID.get(i) + "." + (ID + ""));
                            }
                            if (!found_space) {

                                System.out.println("OVERFLOW PAGE");
                                p.addOverflow(tableName, pagesID.get(i), v);
                                System.out.println("inserted here!!!" + 5 + " " + pagesID.get(i) + "and page count is" + " " + countRows);

                            }
                            DBApp.serialize(p, tableName + "-" + (pagesID.get(i) + ""));
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
                            if (countRows2 == maxRows) {
                                Page p2 = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(i + 1));
                                Vector<Vector<Object>> rows_in_prevPage = p.getRows();
                                Vector<Object> to_be_shifted = rows_in_prevPage.get(p.getNumOfRows() - 1);
                                p2.addRow(to_be_shifted, index);
                                System.out.println("inserted here!!!" + 6 + " " + pagesID.get(i) + "and page count is" + " " + countRows);
                                Vector<Vector<Object>> r = p.getRows();
                                r.remove(p.getNumOfRows() - 1);
                                p.setRows(r);
                                Vector<Object> updatePage = p.addRow(v, index);
                                System.out.println("inserted here!!!" + 7 + " " + pagesID.get(i) + "and page count is" + " " + countRows);
                                pagesInfo.remove(i);
                                pagesInfo.add(i, updatePage);
                                p.sortI(index);
                                DBApp.serialize(p2, tableName + "-" + pagesID.get(i + 1));
                                DBApp.serialize(p, tableName + "-" + pagesID.get(i));
                                break;
                            } else {
                                //create an overflow page and insert into the new page
                                System.out.println("OVERFLOW PAGE");
                                p.addOverflow(tableName, pagesID.get(i), v);
                                System.out.println("inserted here!!!" + 8 + " " + pagesID.get(i) + "and page count is" + " " + countRows);
                                DBApp.serialize(p, tableName + "-" + (pagesID.get(i) + ""));
                                break;
                            }
                        }
                    } else if (countRows < maxRows) {
                        // the pk lies within the range and there is room for it, so we add immediately into the page
                        Vector<Object> updatePage = p.addRow(v, index);
                        pagesInfo.remove(i);
                        pagesInfo.add(i, updatePage);
                        System.out.println("inserted here!!!" + 9 + " " + pagesID.get(i) + "and page count is" + " " + countRows);
                        p.sortI(index);
                        DBApp.serialize(p, tableName + "-" + pagesID.get(i));
                        break;
                    }
                    System.out.println("break here");
                    break;
                } else if (countRows < maxRows) {
                    // pk greater than the max but there is room in the page.
                    Page p = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(i));
                    Vector<Object> updatePage = p.addRow(v, index);
                    pagesInfo.remove(i);
                    pagesInfo.add(i, updatePage);
                    System.out.println("inserted here!!!" + 10 + " " + pagesID.get(i) + "and page count is" + " " + countRows);
                    p.sortI(index);
                    DBApp.serialize(p, tableName + "-" + pagesID.get(i));
                    break;
                }
            }
        }
    }

    public void addPage(Vector<Object> v, int index) throws DBAppException {
        count++;
        Page p = new Page();
        p.setMax_pk_value(v.get(index));
        p.setMin_pk_value(v.get(index));
        p.addRow(v, index);

        this.pagesID.add(count);
        Vector<Object> newPage = new Vector<Object>();
        newPage.add(1);
        newPage.add(v.get(index));
        newPage.add(v.get(index));
        pagesInfo.add(newPage);
        DBApp.serialize(p, tableName + "-" + count + "");
    }

    public int binarySearch(Vector<Vector<Object>> arr, int l, int r, Object x) {
        if (r >= l) {
            int mid = l + (r - l) / 2;

            // Creating an empty enumeration to store
            Enumeration enu = arr.get(mid).elements();

            System.out.println("The enumeration of values are:");

            // Displaying the Enumeration
            while (enu.hasMoreElements()) {
                System.out.println(enu.nextElement());
            }

            // If the element is present at the
            // middle itself <numofPage, min, max>

            Comparable xnew = (Comparable) x;
            Comparable arr_mid_min = (Comparable) (arr.get(mid)).get(1);
            Comparable arr_mid_max = (Comparable) (arr.get(mid)).get(2);
            if (xnew instanceof Date) {
                if ((xnew.compareTo(arr_mid_min)) >= 0 && (arr_mid_max.compareTo(xnew)) >= 0)
                    return mid;
            } else if (Trial.compare(x, (arr.get(mid)).get(1)) >= 0 && Trial.compare((arr.get(mid)).get(2), x) >= 0)
                return mid;

            // If element is smaller than mid, then
            // it can only be present in left subarray
            if (Trial.compare(arr_mid_max, x) > 0)
                return binarySearch(arr, l, mid - 1, x);

            // Else the element can only be present
            // in right subarray
            return binarySearch(arr, mid + 1, r, x);
        }

        // We reach here when element is not present
        // in array
        return -1;
    }

    public void removePage(Page p, int deletePage) {
        pagesInfo.remove(deletePage);
        pagesID.remove(deletePage);
    }

    public void deleteFromPage(Vector<Vector> index_value, int pk_found, Object pk_value) throws DBAppException {
        //binary search
        int c = 0; // this is used to count the number of record deleted. ( the method delete returns boolean, so we count how many trues we have)

        if (pk_value != null) {
            int searchPage = binarySearch(pagesInfo, 0, pagesInfo.size() - 1, pk_value);
            Page p = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(searchPage));
            boolean t = p.deleteRowFromPageB(pk_found, pk_value, index_value);
            if (t) {
                c = c + 1;
            }
            if (p.getNumOfRows() == 0) {
                removePage(p, searchPage);
            } else {
                DBApp.serialize(p, tableName + "-" + pagesID.get(searchPage));
            }
            if (p.getOverFlowInfo() == null) {
                if (c == 0) throw new DBAppException("No such record.");
            } else
                deleteFromOverflowPage(pagesID.get(searchPage), index_value, pk_found, pk_value, c);

        }
        // linear search
        else {
            for (int i = 0; i < pagesID.size(); i++) {
                Page p = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(i));
                boolean t = p.deleteRowFromPageL(pk_found, index_value);
                if (p.getNumOfRows() == 0) {
                    c++;
                    removePage(p, i);
                } else {
                    DBApp.serialize(p, tableName + "-" + pagesID.get(i));
                }
                if (p.getOverFlowInfo() == null) {
                    if (c == 0) throw new DBAppException("No such record.");
                } else
                    deleteFromOverflowPage(pagesID.get(i), index_value, pk_found, pk_value, c);
            }


        }


    }

    public void deleteFromOverflowPage(int pageID, Vector<Vector> index_value, int pk_found, Object pk_value, int c) throws DBAppException {
        //   int c = 0;
        if (pk_value != null) {
            Page p = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(pageID));
//        Page p = (Page) DBApp.deserialize(tableName + "/" + pagesID.get(pageID) + "." + overflowCount);
            Vector<Vector<Object>> overflowPages = p.getOverFlowInfo();
            if (overflowPages != null) {
                for (int i = 0; i < overflowPages.size(); i++) {
                    Page o = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(pageID) + "." + overflowPages.get(i).get(0));
                    if (pk_value != null) {
                        boolean t = o.deleteRowFromPageB(pk_found, pk_value, index_value);
                        if (t) c++;
                    } else {
                        boolean t = o.deleteRowFromPageL(pk_found, index_value);
                        if (t) c++;
                    }
                    if (o.getNumOfRows() == 0) {
                        overflowPages.remove(i);
                    } else {
                        DBApp.serialize(o, tableName + "-" + pagesID.get(pageID) + "." + overflowPages.get(i).get(0));
                    }
                }
                DBApp.serialize(p, tableName + "-" + pagesID.get(pageID));
            }
            if (c == 0) throw new DBAppException("No such record.");
        }
    }

    public int pageSearchSuggestion(Comparable rowKey, int Index_of_Key) throws Exception, FileNotFoundException {
        int lo = 0, hi = pagesID.size() - 1, ans = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >> 1;
            Integer curr = pagesID.get(mid);
            Page currentPage = (Page) DBApp.deserialize(curr + "");
            Vector<Object> startTuple = currentPage.getRows().get(0);
            Comparable startTupleKey = (Comparable) startTuple.get(Index_of_Key);
            if (startTupleKey.compareTo(rowKey) < 0) {
                ans = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return ans;
    }

    public void updateInPage(Vector<Vector> index_value, int pk_found, Object pk_value) throws DBAppException {
        /*
        1- binary search on the page
        2- binary search on the pk_value
        3- update
         */
        System.out.println(pk_value);
        int searchPage = binarySearch(pagesInfo, 0, pagesInfo.size() - 1, pk_value);
        if (searchPage == -1) throw new DBAppException("There is not existing page.");
        Page p = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(searchPage));
        boolean found = p.updateRowInPageB(pk_found, pk_value, index_value);
        DBApp.serialize(p, tableName + "-" + pagesID.get(searchPage));
        if (!found)
            updateInOverflowPage(pagesID.get(searchPage), index_value, pk_found, pk_value);

    }

    private void updateInOverflowPage(Integer pageID, Vector<Vector> index_value, int pk_found, Object pk_value) throws DBAppException {
        if (pk_value != null) {
            Page p = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(pageID));
//        Page p = (Page) DBApp.deserialize(tableName + "/" + pagesID.get(pageID) + "." + overflowCount);

            Vector<Vector<Object>> overflowPages = p.getOverFlowInfo();
            if (overflowPages != null) {
                int countup = 0;
                for (int i = 0; i < overflowPages.size(); i++) {
                    Page o = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(pageID) + "." + overflowPages.get(i).get(0));
                    boolean found = o.updateRowInPageB(pk_found, pk_value, index_value);
                    DBApp.serialize(o, tableName + "-" + pagesID.get(pageID) + "." + overflowPages.get(i).get(0));
                    if (found) {
                        countup++;
                        break;
                    }
                }
                DBApp.serialize(p, tableName + "-" + pagesID.get(pageID));
                if (countup == 0) throw new DBAppException("pk does not exist.");
            }
        }
    }


}