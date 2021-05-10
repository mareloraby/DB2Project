import java.util.Vector;

public class Bucket implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    int MaximumKeysCountinIndexBucket;
    String BucketName;
    int count;
    int noOfEntries;
    Vector<Vector<Object>> addresses; //bucket of entries
    Vector<Vector<Object>> overflowBucketsInfo;


    Bucket(String name) {
        BucketName = name;
        count = 0;
        overflowBucketsInfo = new Vector<Vector<Object>>();
        MaximumKeysCountinIndexBucket = DBApp.MaximumKeysCountinIndexBucket;
        addresses = new Vector<Vector<Object>>();
        noOfEntries = 0;
    }


    public void insertIntoBucket(Object value, String pageName) {//, int rowNumber){
        if (noOfEntries < MaximumKeysCountinIndexBucket) {
            noOfEntries++;
            Vector<Object> entry = new Vector<>();
            entry.add(value);
            entry.add(pageName);
            addresses.add(entry);
        } else { // checking for overflows
            if (overflowBucketsInfo.size() > 0) {
                boolean found = false;
                for (int i = 0; i < overflowBucketsInfo.size(); i++) {
                    Vector<Object> v = overflowBucketsInfo.get(i); // name and num of entries
                    if (Trial.compare(MaximumKeysCountinIndexBucket, v.get(1)) > 0) {
                        found = true;
                        Bucket o = (Bucket) DBApp.deserialize((String) v.get(0));
                        o.insertIntoOverflowBucket(value, pageName);
                        Vector<Object> info = new Vector<Object>();
                        info.add(v.get(0));
                        info.add(1);
                        overflowBucketsInfo.set(i, info);
                    }
                    if (!found) createOverflowBucket(value, pageName);
                }
            } else createOverflowBucket(value, pageName);
        }

    }

    private void insertIntoOverflowBucket(Object value, String pageName) {
        noOfEntries++;
        Vector<Object> entry = new Vector<>();
        entry.add(value);
        entry.add(pageName);
        addresses.add(entry);
    }

    public void createOverflowBucket(Object pk, String pageName) {
        count++;
        String overflowName = BucketName + "-" + count;
        Bucket B = new Bucket(overflowName);
        B.insertIntoOverflowBucket(pk, pageName);
        DBApp.serialize(B, overflowName);
        Vector<Object> info = new Vector<Object>();
        info.add(overflowName);
        info.add(1);
        overflowBucketsInfo.add(info);

    }


}
