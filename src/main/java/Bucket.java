import java.util.Hashtable;
import java.util.Vector;

public class Bucket implements java.io.Serializable {

    private static final long serialVersionUID = 1L;
    private int MaximumKeysCountinIndexBucket;
    private String BucketName;
    private int count;
    private int noOfEntries;

    private Vector<Vector<Object>> addresses; //bucket of entries // pk, pagename

    private Vector<Vector<Object>> overflowBucketsInfo;//


    public String getBucketName() {
        return BucketName;
    }

    public void setBucketName(String bucketName) {
        BucketName = bucketName;
    }

    Bucket(String name) {
        BucketName = name;
        count = 0;
        overflowBucketsInfo = new Vector<Vector<Object>>();
        MaximumKeysCountinIndexBucket = DBApp.MaximumKeysCountinIndexBucket;
        addresses = new Vector<Vector<Object>>();
        noOfEntries = 0;
    }

    private static int bs_next(Vector<Vector<Object>> addresses, int last, Object target) {
        int start = 0, end = last;

        int ans = -1;
        while (start <= end) {
            int mid = (start + end) / 2;

            // Move to right side if target is
            // greater.
            Object pk = (addresses.get(mid)).get(0);
            if (Trial.compare(target, pk) > 0) {
                start = mid + 1;
            }

            // Move left side.
            else {
                ans = mid;
                end = mid - 1;
            }
        }
        return ans;
    }

    public void insertIntoBucket(Object value, String pageName, Hashtable<String, Object> columnNameValues, GridIndex G) {//, int rowNumber){
        if (noOfEntries < MaximumKeysCountinIndexBucket) {
            noOfEntries++;
            Vector<Object> entry = new Vector<>();
            entry.add(value);
            entry.add(pageName);
            // adding the values of the index into addresses vector-> pk, pgName, name value, age value
            for (int j = 0; j < G.getColNames().length; j++) {
                entry.add(columnNameValues.get(G.getColNames()[j]));
            }
            int index = bs_next(addresses, addresses.size() - 2, value);
            if (index == -1) addresses.add(entry);
            else
                addresses.add(index, entry);
        } else { // checking for overflows
            if (overflowBucketsInfo.size() > 0) {
                boolean found = false;
                for (int i = 0; i < overflowBucketsInfo.size(); i++) {
                    // get overflow bucket:
                    Vector<Object> v = overflowBucketsInfo.get(i); // name and num of entries

                    if ((int)v.get(1)< MaximumKeysCountinIndexBucket) {
                        found = true;
                        Bucket o = (Bucket) DBApp.deserialize((String) v.get(0));
                        o.insertIntoOverflowBucket(value, pageName, columnNameValues, G);
                        DBApp.serialize(o, o.BucketName);
                        Vector<Object> info = new Vector<Object>();
                        info.add(v.get(0));
                        info.add((int)v.get(1)+1);
                        overflowBucketsInfo.set(i, info);
                    }
                }
                if (!found) createOverflowBucket(value, pageName, columnNameValues, G);

            } else createOverflowBucket(value, pageName, columnNameValues, G);
        }

    }

    public void deleteFromBucket(Object pk_value) {
        Vector<Object> row_to_be_deleted = binarySearchForDelete(pk_value);
        if (row_to_be_deleted != null) {
            addresses.remove(row_to_be_deleted);
            // delete bucket in case it's empty.
        } else {
            for (int i = 0; i < overflowBucketsInfo.size(); i++) {
                // get overflow bucket:
                Vector<Object> v = overflowBucketsInfo.get(i); // name and num of entries

                Bucket o = (Bucket) DBApp.deserialize((String) v.get(0));
                Vector<Object> row_to_be_deleted2 = o.binarySearchForDelete(pk_value);
                if (row_to_be_deleted2 != null) {
                    o.getAddresses().remove(row_to_be_deleted2);
                    DBApp.serialize(o, o.BucketName);
                    break;
                    // delete bucket in case it's empty.
                }
                DBApp.serialize(o, o.BucketName);
            }
        }
    }


    private void insertIntoOverflowBucket(Object value, String pageName, Hashtable<String, Object> columnNameValues, GridIndex G) {
        noOfEntries++;
        Vector<Object> entry = new Vector<>();
        entry.add(value);
        entry.add(pageName);
        for (int j = 0; j < G.getColNames().length; j++) {
            entry.add(columnNameValues.get(G.getColNames()[j]));
        }
        int index = bs_next(addresses, addresses.size() - 2, value);
        if (index == -1) addresses.add(entry);
        else
            addresses.add(index, entry);
    }

    public void createOverflowBucket(Object pk, String pageName, Hashtable<String, Object> columnNameValues, GridIndex G) {
        count++;
        String overflowName = BucketName + "-" + count;
        Bucket B = new Bucket(overflowName);
        B.insertIntoOverflowBucket(pk, pageName, columnNameValues, G);
        DBApp.serialize(B, overflowName);
        Vector<Object> info = new Vector<Object>();
        info.add(overflowName);
        info.add(1);
        overflowBucketsInfo.add(info);

    }

    public Vector<Object> binarySearchForDelete(Object key) {
        int first = 0;
        int last = this.addresses.size() - 1;
        int mid = (first + last) / 2;
        while (first <= last) {
            //if ( arr[mid] < key )
            Object x = addresses.get(mid).get(0);
            if (Trial.compare(key, x) > 0) {

                first = mid + 1;
            } else if (Trial.compare(key, x) == 0) {
                return addresses.get(mid);

            } else {
                last = mid - 1;
            }
            mid = (first + last) / 2;
        }
        return null;

    }

    public int binarySearch(Object key) {
        int first = 0;
        int last = this.addresses.size() - 1;
        int mid = (first + last) / 2;
        while (first <= last) {
            //if ( arr[mid] < key )
            Object x = addresses.get(mid).get(0);
            if (Trial.compare(key, x) > 0) {

                first = mid + 1;
            } else if (Trial.compare(key, x) == 0) {
                return mid;

            } else {
                last = mid - 1;
            }
            mid = (first + last) / 2;
        }
        return -1;

    }

    public Vector<Vector<Object>> getAddresses() {
        return addresses;
    }

    public void setAddresses(Vector<Vector<Object>> addresses) {
        this.addresses = addresses;
    }


    public Vector<Vector<Object>> getOverflowBucketsInfo() {
        return overflowBucketsInfo;
    }

    public void setOverflowBucketsInfo(Vector<Vector<Object>> overflowBucketsInfo) {
        this.overflowBucketsInfo = overflowBucketsInfo;
    }


}
