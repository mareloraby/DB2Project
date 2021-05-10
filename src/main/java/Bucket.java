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
        } else {
            // check overflow buckets
        }

    }

    public void createOverflowBucket(Object pk, String pageName) {
        count++;
        String overflowName = BucketName + "-" + count;
        Bucket B = new Bucket(overflowName);

        B.insertIntoBucket(pk, pageName);
        DBApp.serialize(B, overflowName);
    }


}
