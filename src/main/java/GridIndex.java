import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Vector;

public class GridIndex implements java.io.Serializable {

    //each cell points to only 1 buckets
    private static final long serialVersionUID = 1L;
    private String GridID;
    private String[] colNames; // columns in grid index
    private String tableName;
    private Vector<Vector<Object>> dimVals;
    // ranges with each cell containing the max of a range
    // 0-10 -> 10; 11-20 -> 20  (dimvals:<X:<10, 20,30...> , Y:<100,200,300,400>, Z:<50,100,150>>) )
    // 20 100 50 , 20 300 50 , 20 400 50, 20 10
    private Vector<String> bucketsinTable;

    //<x:<0,10,20,..>,y:<100,110,120,..>,<z>>

    GridIndex(String tableName, String[] columnNames, int name) throws IOException {
        GridID = name + "";
        dimVals = new Vector<>();
        this.colNames = columnNames;
        this.tableName = tableName;
        int number_of_dimensions = columnNames.length;

        BufferedReader csvReader = null;
        String csvLine;
        try {
            csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        boolean found = false;

        while ((csvLine = csvReader.readLine()) != null) {

            String[] data = csvLine.split(",");
            if (data[0].equals(tableName)) {
                found = true;
                for (int i = 0; i < columnNames.length; i++) {
                    if (data[1].equals(columnNames[i])) {
                        Object minofcol = data[5];
                        Object maxofcol = data[6];
                        // 0-10 , 10-20, 20-30 => <10,20,30>
                        double range = (Trial.compare(maxofcol, minofcol) + 1) / 10;
                        double valSofar = range;
                        Vector<Object> dimension = new Vector<>();
                        for (int j = 0; j < 10; j++) {
                            dimension.add(valSofar);
                            valSofar += range;
                        }
                        dimension.add(null); // to consider null values for each column
                        dimVals.add(dimension);
                    }

                    //<name, age>  => <May, null>
                }


            } else if (data[0] != tableName && found == true)
                break;


        }
        csvReader.close();

    }

    public String getGridID() {
        return GridID;
    }

    public void setGridID(String gridID) {
        GridID = gridID;
    }

    public String[] getColNames() {
        return colNames;
    }

    public Vector<String> getBucketsinTable() {
        return bucketsinTable;
    }

    public void setBucketsinTable(Vector<String> bucketsinTable) {
        this.bucketsinTable = bucketsinTable;
    }

    private static int bs_next(Vector<Object> arr, int last, Object target) {
        int start = 0, end = last;

        int ans = -1;
        while (start <= end) {
            int mid = (start + end) / 2;

            // Move to right side if target is
            // greater.
            if (Trial.compare(target, arr.get(mid)) > 0) {
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

    //  GI dimensions: X:<10,20,30>, Y:<40,50,60>, Z:<100,200,300>
    // where x= 5 and Z= 100

    //Buckets where index of X is 0 and Z is 0 --> 010, 000,020
    // coordinates -> 0,-1,0
    public Vector<String> findAllBuckets(Hashtable<String, Object> colNameValues) {
        Vector<String> b = new Vector<String>();
        Vector<Object> coordinates = new Vector<Object>();
        // tableName-B-coordinates ( X:1, Y:0, Z:2) 1,0,2
        for (int i = 0; i < dimVals.size(); i++) {
            if (colNameValues.contains(colNames[i])) {
                int index = bs_next(dimVals.get(i), dimVals.get(i).size() - 2, colNameValues.get(colNames[i]));
                coordinates.add(index);
            } else
                coordinates.add(-1);
        }
        //Buckets where index of X is 0 and Z is 0 --> 010, 000,020
        // coordinates -> 0,-1,0
        for(int i=0;i<bucketsinTable.size(); i++){
            String bName = bucketsinTable.get(i);
            String[] split1= bName.split("-");
            String[] split2= split1[2].split(",");
            boolean found= true;
            for(int j=0; j<split2.length; j++){
                int cValue= (int)coordinates.get(j);
               if(cValue!=-1 && cValue!=Integer.parseInt(split2[j])){
                   found=false;
                   break;
               }
            }
            if(found){
                b.add(bName); }
        }
        return b;
    }

    // handle nulls ( add null to each dimension)
    public String findCell(Hashtable<String, Object> colNameValues) {
        Vector<Object> coordinates = new Vector<Object>(); //storing the index on the grid where the bucket placed

        for (int i = 0; i < dimVals.size(); i++) {
            if (colNameValues.contains(colNames[i])) {
                int index = bs_next(dimVals.get(i), dimVals.get(i).size() - 2, colNameValues.get(colNames[i]));
                coordinates.add(index);
            } else
                coordinates.add(10);
        }

        StringBuilder indices = new StringBuilder();
        indices.append("-");
        for (int i = 0; i < coordinates.size(); i++) {
            if (i == coordinates.size() - 1) {
                indices.append(coordinates.get(i));
            } else {
                indices.append(coordinates.get(i));
                indices.append(',');
            }
        }
        String BucketName = tableName + "-B" + indices;
        return BucketName;
    }

    public Bucket addBucket(String bucketName) {
        Bucket B = new Bucket(bucketName);
        bucketsinTable.add(bucketName);
        return B;
    }
}


//Insert
/*
 * insert 3adi in the right page
 * store the location
 * search in the grid cells to save the reference
 * check if there is a bucket for it
 * if not create a new one
 * if there is a bucket check if it's not full
 *
 *update position if insert shifts it
 *
 * */


//insert into page
//dawwar 3al range fel grid index for each col
//get to the right cell and check if i have a bucket with this range and insert.
// if the bucket is full create an overflow and insert into it
//if i don't have a bucket, create a new one
