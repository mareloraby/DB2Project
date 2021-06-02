import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class GridIndex implements java.io.Serializable {

    //each cell points to only 1 buckets
    private static final long serialVersionUID = 1L;
    private String GridID;
    private String[] colNames; // columns in grid index
    private String tableName;


    private Vector<Object>[] dimVals;
    // ranges with each cell containing the max of a range
    // 0-10 -> 10; 11-20 -> 20  (dimvals:<X:<10, 20,30...> , Y:<100,200,300,400>, Z:<50,100,150>>) )
    // 20 100 50 , 20 300 50 , 20 400 50, 20 10
    private Vector<String> bucketsinTable;


    private Object[] minOfcols;

    //<x:<0,10,20,..>,y:<100,110,120,..>,<z>>

    GridIndex(String tableName, String[] columnNames, int name) throws IOException {
        GridID = name + "";
        dimVals = new Vector[columnNames.length];
        this.colNames = columnNames;
        this.tableName = tableName;
        int number_of_dimensions = columnNames.length;
        bucketsinTable = new Vector<String>();
        minOfcols = new Object[colNames.length];

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
                        Object minofcol = DBApp.parse(data[2], data[5]);
                        Object maxofcol = DBApp.parse(data[2], data[6]);

                        minOfcols[i] = (minofcol);


                        // 0-10 , 10-20, 20-30 => <10,20,30>

                        double range;
                        if (data[2].equals("java.lang.Double")) {
                            range = Double.parseDouble(data[6]) - Double.parseDouble(data[5]);
                            ;

                        } else if (data[2].equals("java.util.Date")) {
                            range = DBApp.getdifferencedate((data[5]), (data[6]));
                        } else if (data[2].equals("java.lang.String") && data[5].contains("-")) {
                            range = Integer.parseInt(data[6].replace("-", "")) - Integer.parseInt(data[5].replace("-", ""));

                        } else {
                            range = (Trial.compare(maxofcol, minofcol));
                        }

                        range = (range) / 10;
                        double valSofar = range;
                        Vector<Object> dimension = new Vector<>();
                        for (int j = 0; j < 10; j++) {
                            dimension.add(valSofar);
                            valSofar += range;
                        }
                        dimension.add(null); // to consider null values for each column
                        dimVals[i]=(dimension);


                    }
                }


            } else if (!data[0].equals(tableName) && found == true)
                break;


        }
        csvReader.close();
        System.out.println("STARTS HERE");
        System.out.println("THE TABLE NAME IS " + tableName);

        System.out.println("GRID INDEX NAMES" + Arrays.toString(columnNames));
        for (Vector x : dimVals) {
            System.out.println(x.toString());
        }
        System.out.println("ENDS HERE");

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

    public void setColNames(String[] colNames) {
        this.colNames = colNames;
    }

    public Vector<Object>[] getDimVals() {
        return dimVals;
    }

    public void setDimVals(Vector<Object>[] dimVals) {
        this.dimVals = dimVals;
    }

    public Vector<String> getBucketsinTable() {
        return bucketsinTable;
    }

    public void setBucketsinTable(Vector<String> bucketsinTable) {
        this.bucketsinTable = bucketsinTable;
    }

    private static int bs_next(Vector<Object> arr, int last, Object target) {
        int start = 0, end = last;

        int ans = 99;
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
        for (int i = 0; i < dimVals.length; i++) {
            if (colNameValues.containsKey(colNames[i])) {

                Object val = colNameValues.get(colNames[i]);
                double rangeVal;
                if (val instanceof Double) {
                    rangeVal = (Double) val - (Double) minOfcols[i];
                } else if (val instanceof Date) {
                    rangeVal = DBApp.getdifferencedate(DBApp.getLD(minOfcols[i] + ""), DBApp.getLD(val + ""));
                } else if (val instanceof String && ((String) val).contains("-")) {
                    rangeVal = Integer.parseInt((val.toString()).replace("-", "")) - Integer.parseInt((minOfcols[i].toString()).replace("-", ""));

                } else {
                    rangeVal = (Trial.compare(val, minOfcols[i]));
                }

                int index = bs_next(dimVals[i], dimVals[i].size() - 2, rangeVal);


                coordinates.add(index);
            } else
                coordinates.add(-1);
        }

        //Buckets where index of X is 0 and Z is 0 --> 010, 000,020
        // coordinates -> 0,-1,0
        for (int i = 0; i < bucketsinTable.size(); i++) {
            String bName = bucketsinTable.get(i);
            String[] split1 = bName.split("-");
            String[] split2 = split1[2].split(",");
            boolean found = true;
            for (int j = 0; j < split2.length; j++) { // <0,1,2>
                // tablename-B-2,1,2
                int cValue = (int) coordinates.get(j);
                if (cValue != -1 && cValue != Integer.parseInt(split2[j])) {
                    found = false;
                    break;
                }
            }
            if (found) {
                b.add(bName);
            }
        }
        return b;
    }

    // handle nulls ( add null to each dimension)
    public String findCell(Hashtable<String, Object> colNameValues) {
        Vector<Object> coordinates = new Vector<Object>(); //storing the index on the grid where the bucket placed

        for (int i = 0; i < dimVals.length; i++) {
            if (colNameValues.containsKey(colNames[i])) {


                Object val = colNameValues.get(colNames[i]);

                double rangeVal;

                if (val instanceof Double) {
                    rangeVal = (Double) val - (Double) minOfcols[i];
                } else if (val instanceof Date) {
                    rangeVal = DBApp.getdifferencedate(DBApp.getLD(minOfcols[i] + ""), DBApp.getLD(val + ""));
                } else if (val instanceof String && ((String) val).contains("-")) { //id
                    String tem1 = (String) val;
                    String tem2 = "" + (minOfcols[i]);
                    System.out.println(tem2);

                    System.out.println(Arrays.toString(colNames));
                    System.out.println(minOfcols.toString());

                    rangeVal = Integer.parseInt(tem1.replace("-", "")) - Integer.parseInt(tem2.replace("-", ""));

                } else {
                    rangeVal = (Trial.compare(val, minOfcols[i]));
                }

                System.out.println("RANGEVAL 9898HERE");
                System.out.println(rangeVal + " " + colNames[i]);
                System.out.println("dimVals for " + colNames[i] + " " + dimVals[i].toString());
                int index = bs_next(dimVals[i], dimVals[i].size() - 2, rangeVal);

                coordinates.add(index);
            } else
                coordinates.add(10);
        }

        StringBuilder indices = new StringBuilder();
        indices.append("-");
        System.out.println("PLENGTH " + coordinates.size() + " " + colNames.length + " " + Arrays.toString(colNames));
        for (int i = 0; i < coordinates.size(); i++) {
            System.out.println("Pname " + colNames[i]);
            if (i == coordinates.size() - 1) {
                indices.append(coordinates.get(i));
                System.out.println(i + "  fprint indices " + indices);
            } else {
                indices.append(coordinates.get(i));

                System.out.println(i + "  fprint indices " + indices + " jj" + coordinates.get(i));
                indices.append(',');
                System.out.println(i + "  fprint indices " + indices);
            }
            System.out.println(coordinates.size() + " " + i + "  print indices " + indices);
        }
        String BucketName = tableName + "-B" + indices;
        System.out.println(BucketName + " BNAME HERE");
        return BucketName;
    }

    public Bucket addBucket(String bucketName) {
        Bucket B = new Bucket(bucketName);
        bucketsinTable.add(bucketName);
        return B;
    }

    public Object[] getMinOfcols() {
        return minOfcols;
    }

    public void setMinOfcols(Object[] minOfcols) {
        this.minOfcols = minOfcols;
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
