
import java.time.DateTimeException;
import java.util.Hashtable;
import java.util.Vector;

public class Page implements java.io.Serializable {
    private Vector<Vector> rows;
    private Vector<Object> pks;
    private Page overFlow;
    private Object min_pk_value;
    private Object max_pk_value;
    private int numOfRows;

    public void deleteRowFromPage(int pk_found, Object pk_value, Vector<Vector> index_value) {
        Vector<Object> pks = new Vector<Object>();
    }

    public void setOverFlow(Page overFlow) {
        this.overFlow = overFlow;
    }

    public Vector<Vector> getRows() {
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

    private int maxRows;

    //Page.class -> info rows, min,min + row values ?
    public Page() {
        maxRows = DBApp.MaximumRowsCountinPage;
        rows = new Vector<Vector>();
    }

    public void setRows(Vector<Vector> rows) {
        this.rows = rows;
    }

    public Vector<Object> addRow(Vector v, int index) throws DBAppException {
        Vector<Object> info = new Vector<>();
        numOfRows++;
        rows.add(v);
        Object pk = v.get(index);
        if (Trial.compare(pk, max_pk_value) == 1)
            max_pk_value = pk;
        if (Trial.compare(min_pk_value, pk) == 1)
            min_pk_value = pk;

        info.add(numOfRows);
        info.add(min_pk_value);
        info.add(max_pk_value);
        return info;
    }

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

    // called only if there is space
    public void sortB(Object pk_value, Vector<Object> rowAdded) {
        int mid = binarySearch(0, rows.size(), pk_value);
        int temp = mid;
        for (int i = mid + 1; i < rows.size() - 1; i++) {
            pks.setElementAt(pks.get(temp), i + 1);
            rows.setElementAt(rows.get(temp), i + 1);
        }
        pks.setElementAt(pk_value, mid);
        rows.setElementAt(rowAdded, mid);
        updateRowInfo(rowAdded, mid);
    }

// used to sort within a page
    public int binarySearch(int l, int r, Object x) {
        if (r >= l) {
            int mid = l + (r - l) / 2;

            // If the element is present at the
            // middle itself
            int arr_mid = (int) (pks.get(mid));
            if (Trial.compare(arr_mid, x) == 0)
                return mid;

            // If element is smaller than mid, then
            // it can only be present in left subarray
            if (Trial.compare(arr_mid, x) == 1)
                return binarySearch(l, mid - 1, x);

            // Else the element can only be present
            // in right subarray
            return binarySearch(mid + 1, r, x);
        }

        // We reach here when element is not present
        // in array
        return -1;
    }

    public void sortI(int index) {
        Vector<Object> pks = new Vector<>();
        for (int i = 0; i < rows.size(); i++) {
            Vector row = rows.get(i);
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

    public static int compare(Object o1, Object o2) { // compares 2 objects
        if (o1 instanceof Double && o2 instanceof Double) {
            if ((Double) o1 < (Double) o2) {
                return -1;
            } else {
                if ((Double) o1 > (Double) o2) {
                    return 1;
                } else {
                    return 0;
                }
            }
        }
        if (o1 instanceof Integer && o2 instanceof Integer) {
            if ((Integer) o1 < (Integer) o2) {
                return -1;
            } else {
                if ((Integer) o1 > (Integer) o2) {
                    return 1;
                } else {
                    return 0;
                }
            }
        }
        if (o1 instanceof Float && o2 instanceof Float) {
            if ((Float) o1 < (Float) o2) {
                return -1;
            } else {
                if ((Float) o1 > (Float) o2) {
                    return 1;
                } else {
                    return 0;
                }
            }
        } else {
            if (o1 instanceof Boolean && o2 instanceof Boolean) {
                return Boolean.compare((Boolean) o1, (Boolean) o2);
            } else {
                if (o1 instanceof Character && o2 instanceof Character) {
                    return Character.compare((Character) o1, (Character) o2);
                } else {
                    if (o1 instanceof String && o2 instanceof String) {
                        return ((String) o1).compareTo((String) o2);
                    } else {
                        return 30;
                    }
                }
            }
        }


    }

    public static void main(String[] args) {
        // write your code here

    }
}
