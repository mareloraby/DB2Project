import java.util.ArrayList;

public class GridIndex implements java.io.Serializable{

    //each cell points to only 1 buckets
    private static final long serialVersionUID = 1L;

    private ArrayList<ArrayList> cols;

    GridIndex(String tableName, String [] columnNames){

    //insert into page
    //dawwar 3al range fel grid index for each col
    //get to the right cell and check if i have a bucket with this range and insert.
        // if the bucket is full create an overflow and insert into it
    //if i don't have a bucket, create a new one
    for (int i=0; i<columnNames.length; i++){
        cols.add(new ArrayList<Object>());

    }


    }

}


//
