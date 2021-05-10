import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Vector;

public class GridIndex implements java.io.Serializable{

    //each cell points to only 1 buckets
    private static final long serialVersionUID = 1L;
    private Vector<Vector<Object>> dimVals;
    private Vector <String> bucketsinTable;

    //<x:<0,10,20,..>,y:<100,110,120,..>,<z>>

    GridIndex(String tableName, String [] columnNames) throws IOException {
        dimVals = new Vector<>();
        int number_of_dimensions  = columnNames.length;

        BufferedReader csvReader = null;
        String csvLine;
        try {
            csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        boolean found =false;

        while ((csvLine = csvReader.readLine()) != null) {

            String[] data = csvLine.split(",");
            if (data[0].equals(tableName)) {
                found = true;
                for (int i=0; i<columnNames.length;i++){
                    if (data[1].equals(columnNames[i])){
                        Object minofcol = data[5];
                        Object maxofcol = data[6];

                        double range =(Trial.compare(maxofcol,minofcol) +1)/10;
                        Vector <Object> dimension= new Vector<>();
                        for(int j=0; j<10;j++)
                        {
                            dimension.add(range);
                            range+= range;
                        }
                        dimVals.add(dimension);

                    }
                }


            }
            else if (data[0] != tableName && found == true)
                break;


        }
        csvReader.close();

    }

    public Vector<String> getBucketsinTable() {
        return bucketsinTable;
    }

    public void setBucketsinTable(Vector<String> bucketsinTable) {
        this.bucketsinTable = bucketsinTable;
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
