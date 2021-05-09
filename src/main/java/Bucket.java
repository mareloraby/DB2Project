import java.util.Vector;

public class Bucket implements java.io.Serializable {

    private static final long serialVersionUID = 1L;
    int MaximumKeysCountinIndexBucket;
    int noOfEntries;
    Vector <Vector<Object>> addresses; //bucket of entries
    Vector<Object> overflowBuckets;



    Bucket(){
        MaximumKeysCountinIndexBucket = DBApp.MaximumKeysCountinIndexBucket;
        addresses = new Vector<Vector<Object>>();
        noOfEntries = 0;
    }

    void insertIntoBucket(Object value, String pageName, int rowNumber){
        noOfEntries++;

        Vector <Object> entry = new Vector<>();
        entry.add(value);
        entry.add(pageName);
        entry.add(rowNumber);

        addresses.add(entry);

    }





}
