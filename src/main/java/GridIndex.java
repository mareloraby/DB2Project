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
    private Vector<String> bucketsinTable;
    private Object[] minOfcols;

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

                        double range;
                        if (data[2].equals("java.lang.Double")) {
                            range = Double.parseDouble(data[6]) - Double.parseDouble(data[5]);

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


                    rangeVal = Integer.parseInt(tem1.replace("-", "")) - Integer.parseInt(tem2.replace("-", ""));

                } else {
                    rangeVal = (Trial.compare(val, minOfcols[i]));
                }


                int index = bs_next(dimVals[i], dimVals[i].size() - 2, rangeVal);

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
        String BucketName = tableName + "-GI"+GridID+"B" +  indices;
        return BucketName;
    }
    public Bucket addBucket(String bucketName) {
        Bucket B = new Bucket(bucketName);
        bucketsinTable.add(bucketName);
        return B;
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

    public Object[] getMinOfcols() {
        return minOfcols;
    }

    public void setMinOfcols(Object[] minOfcols) {
        this.minOfcols = minOfcols;
    }

}

