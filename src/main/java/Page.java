
import java.util.*;

public class Page implements java.io.Serializable {

    private Vector<Vector<Object>> rows;

    public void setMin_pk_value(Object min_pk_value) {
        this.min_pk_value = min_pk_value;
    }

    public void setMax_pk_value(Object max_pk_value) {
        this.max_pk_value = max_pk_value;
    }

    private Vector<Object> pks;

    public Vector<Vector<Object>> getOverFlowInfo() {
        return overFlowInfo;
    }

    public void setOverFlowInfo(Vector<Vector<Object>> overFlowInfo) {
        this.overFlowInfo = overFlowInfo;
    }

    private static final long serialVersionUID = 1L;
    private Page overFlow;
    private int count;
    private int pk_index;
    private Vector<Vector<Object>> overFlowInfo; //stores ID and number of rows
    private Object min_pk_value;

    public int getPk_index() {
        return pk_index;
    }

    // method to get a vector of object primary keys directly from rows
    // this method is needed in binary search here because we did not add pks to pks vector of overflow page
    // so we construct the pk vector from the available pages rows
    public void getPks() {
        pks = new Vector<>();
        for (int i = 0; i < rows.size(); i++) {
            Vector<Object> row = rows.get(i);
            pks.add(row.get(pk_index));
        }
    }

    public void setPk_index(int pk_index) {
        this.pk_index = pk_index;
    }

    private Object max_pk_value;
    private int numOfRows;


    //Page.class -> info rows, min,min + row values ?
    public Page() {
        pk_index = 0;
        overFlowInfo = new Vector<Vector<Object>>();
        min_pk_value = null;
        max_pk_value = null;
        maxRows = DBApp.MaximumRowsCountinPage;
        rows = new Vector<Vector<Object>>();
        pks = new Vector<Object>();
        count = 0;
        numOfRows = 0;
    }

    // called only if there is space
    public void sortB(Object pk_value) {
        int mid = binarySearch(0, pks.size() - 1, pk_value);
        int temp = mid;
        for (int i = pks.size() - 1; i > mid; i++) {
            pks.setElementAt(pks.get(i), i + 1);
        }
        pks.setElementAt(pk_value, mid);

    }

    //0,1
    // 15,20 check ..!
    public Vector<Object> addRow(Vector v, int index) throws DBAppException {
        Vector<Object> info = new Vector<>();
        numOfRows++;
        rows.add(v);
        pks.add(v.get(index));
        sortI(index);
//        Collections.sort(pks);
        Object pk = v.get(index);
        if (Trial.compare(pk, max_pk_value) > 0)
            max_pk_value = pk;
        if (Trial.compare(min_pk_value, pk) > 0)
            min_pk_value = pk;

        info.add(numOfRows);
        info.add(min_pk_value);
        info.add(max_pk_value);
        return info;
    }

    // create overflow page
    public void addOverflow(String tableName, int PageID, Vector<Object> v, int index) throws DBAppException {
        count++;
        Page p = new Page();
        p.setPk_index(index);
        System.out.println("OVERFLOW NEW SETTING PK:"+p.getPk_index());
        int n = p.addOverflowRow(v);
        Vector<Object> newPage = new Vector<Object>();
        newPage.add(count);
        newPage.add(n);
        overFlowInfo.add(newPage);
        System.out.println(tableName + " " + PageID + " " + count);
        DBApp.serialize(p, tableName + "-" + PageID + "." + count);
    }

    // add a row to an overflow page
    public int addOverflowRow(Vector v) throws DBAppException {
        numOfRows++;
        rows.add(v);
//        pks.add(v.get(index));
//        sortI(index);
        return numOfRows;
    }


    // used to sort within a page
    public int binarySearch(int l, int r, Object x) {
        // Creating an empty enumeration to store
     /*   Enumeration enu = pks.elements();

        System.out.println("The enumeration of values are:");

        // Displaying the Enumeration
        while (enu.hasMoreElements()) {
            System.out.println(enu.nextElement());
        }      */
        getPks();
        if (r >= l) {
            int mid = l + (r - l) / 2;
            // If the element is present at the
            // middle itself
            Object arr_mid = (Object) (pks.get(mid));
            if (Trial.compare(arr_mid, x) == 0)
                return mid;

            // If element is smaller than mid, then
            // it can only be present in left subarray
            if (Trial.compare(arr_mid, x) > 0)
                return binarySearch(l, mid - 1, x);

            // Else the element can only be present
            // in right subarray
            return binarySearch(mid + 1, r, x);
        }
        // We reach here when element is not present
        // in array

        return -1;
    }

    public boolean deleteRowFromPageB(int pk_found, Object pk_value, Vector<Vector> index_value) throws DBAppException {

        int mid = binarySearch(0, rows.size() - 1, pk_value);
        // Creating an empty enumeration to store
        Enumeration enu = pks.elements();
        System.out.println("1234");
        System.out.println(pk_value.toString() + "  " + index_value.get(0).get(0) + " " + index_value.get(0).get(1));
        System.out.println("INDEX VALUES");
        Enumeration enu1 = index_value.elements();
        while (enu1.hasMoreElements()) {
            System.out.println(enu1.nextElement());
        }
        System.out.println(mid);
        System.out.println("The enumeration of values are:");
        int countx = 0;
        // Displaying the Enumeration
        while (enu.hasMoreElements()) {
            System.out.println(enu.nextElement());
            countx++;
        }
        System.out.println(countx + " no of rows ");
        if (mid == -1) return false;
        Vector<Object> row = rows.get(mid);
        boolean found = true;
        for (int i = 0; i < index_value.size(); i++) {
            System.out.println("123");
            int rowToDeleteIndex = (int) index_value.get(i).get(0);
            Object rowToDeleteValue = index_value.get(i).get(1);

//            Comparable rowToDeleteValuen = (Comparable) rowToDeleteValue;
//            Comparable tempr = (Comparable) row.get(rowToDeleteIndex);
            if (Trial.compare(rowToDeleteValue, row.get(rowToDeleteIndex)) == 0) {
                System.out.println("12");

            } else {
                found = false;
            }
//                throw new DBAppException("There is not existing row in the page with the same values.");

        }
        if (found) {
            rows.remove(mid);
            System.out.println("DeleteCheck");
            numOfRows--;
            if (rows.size() > 0) {
                int last_index = rows.size() - 1;
                max_pk_value = rows.get(last_index).get(pk_found);
                min_pk_value = rows.get(0).get(pk_found);
            }
            return true;
        }
        return false;
    }

    public void updatePageAfterDelete(int pk_found) {
//        System.out.println(pk_found);
        numOfRows--;
        if (rows.size() > 0) {
            int last_index = rows.size() - 1;
            max_pk_value = rows.get(last_index).get(pk_found);
            min_pk_value = rows.get(0).get(pk_found);
        }
    }

    public boolean deleteRowFromPageL(int pk_found, Vector<Vector> index_value) {
        Vector<Integer> deletedRowsIndex = new Vector<Integer>();
        int c = 0;
        for (int i = 0; i < rows.size(); i++) {
            Vector<Object> row = rows.get(i);
            boolean perfectMatch = true;
            for (int j = 0; j < index_value.size(); j++) {
                int rowToDeleteIndex = (int) index_value.get(j).get(0);
                Object rowToDeleteValue = index_value.get(j).get(1);
               // Trial.compare(rowToDeleteValue, row.get(rowToDeleteIndex)) == 0
                if (compare(row.get(rowToDeleteIndex), rowToDeleteValue) == 0) {
                    continue;
                } else {
                    perfectMatch = false;
                    break;
                }
            }
            // if it fully matches the value sin index_value , remove from the page
            if (perfectMatch) {
                System.out.println("Delete Check");
                c++;
                deletedRowsIndex.add(i);
            }
        }
        if (c == 0) return false;
        else {

//            int i=0;
//            while(true){
//                int idx = deletedRowsIndex.get(i);
//                Vector<Object> removed = rows.get(idx);//henaaaa
//                rows.remove(removed);
//
//                i++;
//
//
//            }


// index in rows is shifted so we loop
            for (int i = deletedRowsIndex.size()-1; i>=0; i--){
//                for (int i = 0; i<deletedRowsIndex.size(); i++){
                Vector<Object> removed = rows.get(deletedRowsIndex.get(i));//henaaaa
//                deletedRowsIndex.remove(i);
                rows.remove(removed);
                updatePageAfterDelete(pk_found);



            }

//
//            for (int i = 0; i < deletedRowsIndex.size(); i++) {
//                countdelete--;
//                if (countdelete == -1){break;}
              //  System.out.println(deletedRowsIndex.get(i) +" deleted row indx ");
//                Vector<Object> removed = rows.get(deletedRowsIndex.get(i));//henaaaa
//                deletedRowsIndex.remove(i);
                //i--;
                //rows.remove(removed);


                //if (i == (deletedRowsIndex.size()-1)){break;}

//            System.out.print("DELETED ROWS IN THE PAGE");
//            Enumeration enu1 = removed.elements();
//            while (enu1.hasMoreElements()) {
//                System.out.print(enu1.nextElement()+" ");
//            }
//            System.out.println();
//            updatePageAfterDelete(pk_found);
            //}

            return true;
        }


    }

    public void setOverFlow(Page overFlow) {
        this.overFlow = overFlow;
    }

    public Vector<Vector<Object>> getRows() {
        return rows;
    }

    public Page getOverFlow() {
        return overFlow;
    }

    public Object getMin_pk_value() {
        return min_pk_value;
    }

    public Object getMax_pk_value() {
        return max_pk_value;
    }

    public int getNumOfRows() {
        return numOfRows;
    }

    public int getMaxRows() {
        return maxRows;
    }

    public void setNumOfRows(int numOfRows) {
        this.numOfRows = numOfRows;
    }

    private int maxRows;


    public void setRows(Vector<Vector<Object>> rows) {
        this.rows = rows;
    }


    /*
    page full -> access overflow pages -> add row in one of the overflow pages- >
    page.getOverflowInfo ( check num of rows)
    overflow page has space

    add vector in the overflow page :
    * update overflowInfo vector of the current main page ** overflow.getNumofRows()**
    * update the num of rows in the overflow page


    */
    // used with sortB to update info in the page
    public void updateRowInfo(Vector v, int index) {
        Vector<Object> info = new Vector<>();
        numOfRows++;
        Object pk = v.get(index);
        if (Trial.compare(pk, max_pk_value) == 1)
            max_pk_value = pk;
        if (Trial.compare(min_pk_value, pk) == 1)
            min_pk_value = pk;
        info.add(numOfRows);
        info.add(min_pk_value);
        info.add(max_pk_value);
    }

//    // called only if there is space
//    public void sortB(Object pk_value, Vector<Object> rowAdded) {
//        int mid = binarySearch(0, rows.size(), pk_value);
//        int temp = mid;
//        for (int i = mid + 1; i < rows.size() - 1; i++) {
//            pks.setElementAt(pks.get(temp), i + 1);
//            rows.setElementAt(rows.get(temp), i + 1);
//        }
//        pks.setElementAt(pk_value, mid);
//        rows.setElementAt(rowAdded, mid);
//        updateRowInfo(rowAdded, mid);
//    }


    public void sortI(int index) {
        pks = new Vector<>();
        for (int i = 0; i < rows.size(); i++) {
            Vector<Object> row = rows.get(i);
            pks.add(row.get(index));
        }

        for (int i = 1; i < pks.size(); i++) {
            // da in case pks.size()= v.size() => insertion sort
            Object value = pks.get(i);
            Vector<Object> o = rows.get(i);
            int j;
            for (j = i - 1; j >= 0 && ((compare(pks.get(j), value) > 0)); j--) {
                pks.setElementAt(pks.get(j), j + 1);
                rows.setElementAt(rows.get(j), j + 1);
            }
            pks.setElementAt(value, j + 1);
            rows.setElementAt(o, j + 1);
        }
    }

    public boolean updateRowInPageB(int pk_found, Object pk_value, Vector<Vector> index_value) throws DBAppException {
        int mid = binarySearch(0, rows.size() - 1, pk_value);
        if (mid == -1) return false;
        Vector<Object> row = rows.get(mid);
        for (int i = 0; i < index_value.size(); i++) {
            System.out.println("UPDATED OVERFLOW2");
            int rowToUpdateIndex = (int) index_value.get(i).get(0);
            Object rowToUpdateValue = index_value.get(i).get(1);
            row.set(rowToUpdateIndex, rowToUpdateValue);
        }
        rows.set(mid, row);
        return true;
    }

    public static int compare(Object o1, Object o2) {

        return (((Comparable) o1).compareTo((Comparable) o2));

        // compares 2 objects
//
//        if (o1 instanceof Date && o2 instanceof Date) {
//
//            if (((Date) o1).compareTo((Date) o2) > 0) return 1; // o1 appears after o2
//            else if (((Date) o1).compareTo((Date) o2) < 0) return -1;
//            else return 0;
//
//        }
//        if (o1 instanceof Double && o2 instanceof Double) {
//            if ((Double) o1 < (Double) o2) {
//                return -1;
//            } else {
//                if ((Double) o1 > (Double) o2) {
//                    return 1;
//                } else {
//                    return 0;
//                }
//            }
//        }
//        if (o1 instanceof Integer && o2 instanceof Integer) {
//            if ((Integer) o1 < (Integer) o2) {
//                return -1;
//            } else {
//                if ((Integer) o1 > (Integer) o2) {
//                    return 1;
//                } else {
//                    return 0;
//                }
//            }
//        }
//        if (o1 instanceof Float && o2 instanceof Float) {
//            if ((Float) o1 < (Float) o2) {
//                return -1;
//            } else {
//                if ((Float) o1 > (Float) o2) {
//                    return 1;
//                } else {
//                    return 0;
//                }
//            }
//        } else {
//            if (o1 instanceof Boolean && o2 instanceof Boolean) {
//                return Boolean.compare((Boolean) o1, (Boolean) o2);
//            } else {
//                if (o1 instanceof Character && o2 instanceof Character) {
//                    return Character.compare((Character) o1, (Character) o2);
//                } else {
//                    if (o1 instanceof String && o2 instanceof String) {
//                        return ((String) o1).compareTo((String) o2);
//                    } else {
//                        return 30;
//                    }
//                }
//            }

    }

    public static void main(String[] args) {
        // write your code here

    }


}
