import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

public class Table implements java.io.Serializable {

    private static final long serialVersionUID = 1L;
    private String tableName;
    private int pk_index;
    private int count;
    private Vector<Vector<Object>> pagesInfo;
    private ArrayList<String> colNamesTable;
    private Vector<Object> pks;
    private Vector<Integer> pagesID;
    private int maxRows;
    private Vector<GridIndex> gridIndices;
    private boolean hasGrid;


    public Table(String name, int pk_index) {
        this.pk_index = pk_index;
        maxRows = DBApp.MaximumRowsCountinPage;
        pagesID = new Vector<Integer>();
        pagesInfo = new Vector<Vector<Object>>();
        pks = new Vector<Object>();
        tableName = name;
        count = 0;
        gridIndices = new Vector<GridIndex>();
        hasGrid = false;
        colNamesTable = new ArrayList<String>();

    }


    public int getCount() {
        return count;
    }

    public Vector<GridIndex> getGridIndices() {
        return gridIndices;
    }

    public void setGridIndices(Vector<GridIndex> gridIndices) {
        this.gridIndices = gridIndices;
    }


    public Vector<Vector<Object>> selectfromTable(Hashtable<String, Object> colNameValue, Hashtable<String, String> colNameOperator) {
        GridIndex G = chooseIndex(tableName, colNameValue); // best index : age , name , gpa --> name , age
        Vector<Vector<Object>> rows = new Vector<Vector<Object>>();
        if(G ==  null){
            int index = 0;
            String key = new String(); //
            String OP = new String();
            for (int j = 0; j < colNamesTable.size(); j++) {
                if ((colNameValue.containsKey(colNamesTable.get(j)))) {
                    index = j;
                    key = colNamesTable.get(j);
                    OP = colNameOperator.get(colNamesTable.get(j));
                }
            }

            for(int i=0; i<pagesInfo.size(); i++) {
                Vector<Object> page = pagesInfo.get(i);
                Page p = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(i));
                for(int j=0; j<p.getRows().size(); j++){
                    Vector<Object> v = p.getRows().get(j);
                    switch (OP) {
                        case ">":
                            if (Trial.compare(v.get(index), colNameValue.get(key)) > 0) {
                                rows.add(v);
                            }
                            break;
                        case ">=":
                            if (Trial.compare(v.get(index), colNameValue.get(key)) >= 0) {
                                rows.add(v);
                            }
                            break;
                        case "<":
                            if (Trial.compare(v.get(index), colNameValue.get(key)) < 0) {
                                rows.add(v);
                            }
                            break;
                        case "<=":
                            if (Trial.compare(v.get(index), colNameValue.get(key)) <= 0) {
                                rows.add(v);
                            }
                            break;
                        case "!=":
                            if (Trial.compare(v.get(index), colNameValue.get(key)) != 0) {
                                rows.add(v);
                            }
                            break;
                        case "=":
//                        System.out.println("firstname string check "+v.get(index)+" "+ colNameValue.get(key));
                            if (Trial.compare(v.get(index), colNameValue.get(key)) == 0) {
                                System.out.println("firstname string check " + v.get(index) + " " + colNameValue.get(key));
                                rows.add(v);
                            }
                            break;

                    }
                            }
                if (p.getOverFlowInfo().size() != 0) {
                    Vector<Vector<Object>> overflowPagesInfo = p.getOverFlowInfo();
                    for (int k = 0; k < p.getOverFlowInfo().size(); k++) {
                        Vector<Object> overflow = overflowPagesInfo.get(k);
                        int ID = (int) overflow.get(0);
                        Page o = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(i) + "." + ID);
                        Vector<Vector<Object>> rowsOverflow = o.getRows();
                        System.out.println("PageOVERFLOW" + pagesID.get(i) + " " + ID + " " + "in " + tableName + " with pagesCount " + overflow.get(1));
                        for (int l = 0; l < o.getRows().size(); l++) {
                            Vector<Object> v = p.getRows().get(l);
                            switch (OP) {
                                case ">":
                                    if (Trial.compare(v.get(index), colNameValue.get(key)) > 0) {
                                        rows.add(v);
                                    }
                                    break;
                                case ">=":
                                    if (Trial.compare(v.get(index), colNameValue.get(key)) >= 0) {
                                        rows.add(v);
                                    }
                                    break;
                                case "<":
                                    if (Trial.compare(v.get(index), colNameValue.get(key)) < 0) {
                                        rows.add(v);
                                    }
                                    break;
                                case "<=":
                                    if (Trial.compare(v.get(index), colNameValue.get(key)) <= 0) {
                                        rows.add(v);
                                    }
                                    break;
                                case "!=":
                                    if (Trial.compare(v.get(index), colNameValue.get(key)) != 0) {
                                        rows.add(v);
                                    }
                                    break;
                                case "=":
//                        System.out.println("firstname string check "+v.get(index)+" "+ colNameValue.get(key));
                                    if (Trial.compare(v.get(index), colNameValue.get(key)) == 0) {
                                        System.out.println("firstname string check " + v.get(index) + " " + colNameValue.get(key));
                                        rows.add(v);
                                    }
                                    break;

                            }
                        }
                        DBApp.serialize(o, tableName + "-" + pagesID.get(i) + "." + (ID + ""));
                    }
                }
                DBApp.serialize(p, tableName + "-" + pagesID.get(i));
                }
            }



        else {
            Vector<Vector<Object>> coordinates = new Vector<Vector<Object>>();


            String OP = new String();
            for (int i = 0; i < G.getDimVals().length; i++) { // name = ahmed ,age
                if (colNameValue.containsKey(G.getColNames()[i])) {
                    String operator = colNameOperator.get(G.getColNames()[i]);
                    OP = operator;
                    Vector<Object> dimValCol = G.getDimVals()[i]; // ""
                    Vector<Object> temp = new Vector<Object>();

                    Object val = colNameValue.get(G.getColNames()[i]);
                    double rangeVal;
                    if (val instanceof Double) {
                        rangeVal = (Double) val - (Double) G.getMinOfcols()[i];
                    } else if (val instanceof Date) {
                        rangeVal = DBApp.getdifferencedate(DBApp.getLD(G.getMinOfcols()[i] + ""), DBApp.getLD(val + ""));
                    } else if (val instanceof String && ((String) val).contains("-")) {
                        rangeVal = Integer.parseInt((val.toString()).replace("-", "")) - Integer.parseInt((G.getMinOfcols()[i].toString()).replace("-", ""));

                    } else {
                        rangeVal = (Trial.compare(val, G.getMinOfcols()[i]));
                    }

                    switch (operator) {
                        case ">":
                            for (int j = 0; j < dimValCol.size() - 1; j++) {
                                if (Trial.compare(dimValCol.get(j), rangeVal) > 0)
                                    temp.add(j);
                            }
                            break;
                        case ">=":
                            for (int j = 0; j < dimValCol.size() - 1; j++) {
                                if (Trial.compare(dimValCol.get(j), rangeVal) >= 0)
                                    temp.add(j);
                            }
                            break;
                        case "<":
                            for (int j = 0; j < dimValCol.size() - 1; j++) {
                                if (j < dimValCol.size() && Trial.compare(dimValCol.get(j), rangeVal) < 0) {
                                    temp.add(j);
//                                temp.add(j + 1);
                                }


                            }
                            break;
                        case "<=":
                            for (int j = 0; j < dimValCol.size() - 1; j++) {
                                if (Trial.compare(dimValCol.get(j), rangeVal) <= 0)
                                    temp.add(j);
                            }
                            break;
                        case "!=":
                            for (int j = 0; j < dimValCol.size() - 1; j++) {
                                if (Trial.compare(dimValCol.get(j), rangeVal) != 0)
                                    temp.add(j);
                            }
                            break;
                        case "=":
                            System.out.println("here plz");

                            for (int j = 0; j < dimValCol.size() - 2; j++) { // name
                                if (j == 0 && (Trial.compare(dimValCol.get(j), rangeVal) >= 0)) {
                                    temp.add(j);
                                    System.out.println("here plz3");

                                } else if ((Trial.compare(dimValCol.get(j), rangeVal) < 0) && (Trial.compare(dimValCol.get(j + 1), rangeVal) >= 0)) {
                                    System.out.println("here plz4");

                                    temp.add(j + 1);
                                }

                            }

                            break;

                    }
//                int index = bs_next(dimVals.get(i), dimVals.get(i).size() - 2, colNameValues.get(colNames[i]));
                    coordinates.add(temp);
                } else {
                    Vector<Object> emptyDummy = new Vector<Object>();
                    coordinates.add(emptyDummy);
                }
            }

            Vector<String> bucketNs = new Vector<String>();
            Vector<String> bucketsinTable = G.getBucketsinTable();
            for (int k = 0; k < coordinates.size(); k++) {
                //   Vector<String> temp = new Vector<String>();
                Vector<Object> cValue = coordinates.get(k);
                for (int i = 0; i < bucketsinTable.size(); i++) {
                    String bName = bucketsinTable.get(i);
                    String[] split1 = bName.split("-");
                    String[] split2 = split1[2].split(",");
                    boolean found = true;
                    if (cValue.size() != 0 && !cValue.contains(Integer.parseInt(split2[k]))) {
                        found = false;
                        break;
                    }
                    if (found) {

                        bucketNs.add(bName);
                    }
                }
            }
            // <<Bname>,<>>
            Vector<String> pname = new Vector<String>();

            for (int i = 0; i < bucketNs.size(); i++) {
                //   Vector<String> BN =  bucketNs.get(i);
                // for(int j =0; j<BN.size(); j++){
                String Bucket = bucketNs.get(i);
                Bucket b1 = (Bucket) DBApp.deserialize(Bucket);
                for (int k = 0; k < b1.getAddresses().size(); k++) {
                    String page = (String) b1.getAddresses().get(k).get(1);
                    if (!pname.contains(page))
                        pname.add(page);
                }
                for (int x = 0; x < b1.getOverflowBucketsInfo().size(); x++) {
                        // get overflow bucket:
                        Vector<Object> v = b1.getOverflowBucketsInfo().get(i); // name and num of entries
                        Bucket o = (Bucket) DBApp.deserialize((String) v.get(0));
                    for (int m = 0; m < o.getAddresses().size(); m++) {
                        String page = (String) o.getAddresses().get(m).get(1);
                        if (!pname.contains(page))
                            pname.add(page);
                    }
                    DBApp.serialize(o, o.getBucketName());
                }
                DBApp.serialize(b1, b1.getBucketName());
                // }
            }

            int index = 0;
            String key = new String();
            for (int j = 0; j < colNamesTable.size(); j++) {
                if ((colNameValue.containsKey(colNamesTable.get(j)))) {
                    index = j;
                    key = colNamesTable.get(j);
                }
            }

            for (int i = 0; i < pname.size(); i++) {
                //for(int i=0; i<pname.get(x).size(); i++){
                Page p = (Page) DBApp.deserialize(pname.get(i));
                for (int k = 0; k < p.getRows().size(); k++) {

                    Vector<Object> v = p.getRows().get(k);
//                System.out.println("firstname string check "+OP+" "+ v.get(index)+" "+ colNameValue.get(key)+"  "+ key);
                    switch (OP) {
                        case ">":
                            if (Trial.compare(v.get(index), colNameValue.get(key)) > 0) {
                                rows.add(v);
                            }
                            break;
                        case ">=":
                            if (Trial.compare(v.get(index), colNameValue.get(key)) >= 0) {
                                rows.add(v);
                            }
                            break;
                        case "<":
                            if (Trial.compare(v.get(index), colNameValue.get(key)) < 0) {
                                rows.add(v);
                            }
                            break;
                        case "<=":
                            if (Trial.compare(v.get(index), colNameValue.get(key)) <= 0) {
                                rows.add(v);
                            }
                            break;
                        case "!=":
                            if (Trial.compare(v.get(index), colNameValue.get(key)) != 0) {
                                rows.add(v);
                            }
                            break;
                        case "=":
//                        System.out.println("firstname string check "+v.get(index)+" "+ colNameValue.get(key));
                            if (Trial.compare(v.get(index), colNameValue.get(key)) == 0) {
                                System.out.println("firstname string check " + v.get(index) + " " + colNameValue.get(key));
                                rows.add(v);
                            }
                            break;

                    }

                }
                DBApp.serialize(p, pname.get(i));

            }
            DBApp.serialize(G, tableName + "-GI" + G.getGridID());
            System.out.println(rows.size() + "   rows printed");
        }
        return rows;

    }


    //100-200, 300-400 (both are full and I want to insert 250)
    public Vector<Vector<Object>> updatePageOverflowInfo(Page p, Page o, int j) {
        Vector<Vector<Object>> oldOverflowInfoPages = p.getOverFlowInfo();
        Vector<Object> oldOverflow = oldOverflowInfoPages.get(j);
        oldOverflow.setElementAt(o.getNumOfRows(), 1);
        oldOverflowInfoPages.setElementAt(oldOverflow, j);
        return oldOverflowInfoPages;
    }

    public void checktablePage() {
        for (int i = 0; i < (pagesID).size(); i++) {
            Vector<Object> page = pagesInfo.get(i);

            Enumeration enu = pagesInfo.get(i).elements();
            System.out.println("Pages " + pagesID.get(i) + " " + "in " + tableName + " with pagesCount " + pagesInfo.get(i).get(0));
            // Displaying the Enumeration
            while (enu.hasMoreElements()) {
                System.out.println(enu.nextElement());
            }
        }
    }

    public void printPages() {
        for (int i = 0; i < (pagesID).size(); i++) {
            Vector<Object> page = pagesInfo.get(i);
            Page p = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(i));
            Enumeration enu2 = page.elements();
            System.out.println("Pages info for" + pagesID.get(i));
            while (enu2.hasMoreElements()) {
                System.out.print(enu2.nextElement() + " ");
            }
            System.out.println();
            Vector<Vector<Object>> r = p.getRows();
            System.out.println("Page " + pagesID.get(i) + " " + "in " + tableName + " with pagesCount " + pagesInfo.get(i).get(0));
            // rows of page:
            for (int j = 0; j < r.size(); j++) {
                Enumeration enu = r.get(j).elements();
//                // Displaying the Enumeration
                while (enu.hasMoreElements()) {
                    System.out.println(enu.nextElement() + " ");
                }
            }
            System.out.println();
            // check if this page has overflows:
            if (p.getOverFlowInfo().size() != 0) {
                Vector<Vector<Object>> overflowPagesInfo = p.getOverFlowInfo();
                for (int k = 0; k < p.getOverFlowInfo().size(); k++) {
                    Vector<Object> overflow = overflowPagesInfo.get(k);
                    int ID = (int) overflow.get(0);
                    Page o = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(i) + "." + ID);
                    Vector<Vector<Object>> rowsOverflow = o.getRows();
                    System.out.println("PageOVERFLOW" + pagesID.get(i) + " " + ID + " " + "in " + tableName + " with pagesCount " + overflow.get(1));
                    for (int l = 0; l < o.getRows().size(); l++) {
                        Enumeration enu1 = rowsOverflow.get(l).elements();

                        // Displaying the Enumeration
                        while (enu1.hasMoreElements()) {
                            System.out.println(enu1.nextElement());

                        }
                    }
                }
            }
        }
    }

    public void insertIntoPage(Vector<Object> v, int index) throws DBAppException {
        checktablePage();

        // check if the page is full, if yes, go to the next page and check whether it has space
        // if the next page has space, insert into the second page and do the needed shifting
        // otherwise create an overflow in the initial page we wanted to insert in
        // update min/max if needed
        // insert into the right page then call sortI


        // get the clusteringKey ck of v
        Object pk = v.get(index);
        System.out.println("THE VALUE TO BE INSERTED IS " + pk);
        // check whether the table has pages
        // if there is no page, create a new one and insert
        // insert the info related to this page into the pagesInfo Vector
        if (count == 0) {
            addPage(v, index);
            System.out.println("the amazing searchKey " + " to insert: " + pk);
            if (pks.contains(pk))
                throw new DBAppException("You can not insert a duplicate key for a primary key.");
            else {
                pks.add(pk);
            }

        } else {

            // check if pk already exists in the table
            if (pks.contains(pk))
                throw new DBAppException("You can not insert a duplicate key for a primary key.");
            else {
                pks.add(pk);
            }

// 0-100(has no space) , 120-130(has space)
// I want to insert 30, but 30 should not go into 120-130!, thus I have to insert in the prev page(0-100)
// which means I need to shift one record from 0-100 to 120-130 and insert into 0-100
// however if 120-130 is also full, create an overflow page for 0-100
            System.out.println("Binary maxes");
            for (int m = 0; m < pagesID.size(); m++) {
                System.out.print(pagesInfo.get(m).get(2) + " ");
            }
            System.out.println();
            int searchPage = binarySearch(pagesInfo, 0, pagesInfo.size() - 1, pk);
            // check whether the pk fits within the ranges of min-max of any existing page
            int i = searchPage;
            while (true) {
                System.out.println(i);
                if (i == -1) {
                    System.out.println("ur i is -1 ");
                    i = pagesID.size() - 1;
                }
                System.out.println("the amazing searchKey " + i + " to insert: " + pk);
                // retrieve the info of the page we're standing at
                Vector<Object> page = pagesInfo.get(i);

                Enumeration enu = pagesInfo.get(i).elements();
                System.out.println("The enumeration of values are:");
                // Displaying the Enumeration
                while (enu.hasMoreElements()) {
                    System.out.println(enu.nextElement());
                }

                int countRows = (int) page.get(0); // <"2,0,10000", >
                Object min = page.get(1);
                Object max = page.get(2);

                // if we are on the last page in the table
                if (i == (pagesID.size() - 1)) {

                    // check whether we have room for a new row in the last page.If so, add the new row.
                    if (countRows < maxRows) {
                        Page p = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(i));
                        // after adding the new row, update the page info and add it into the pagesInfo Vector in the table.
                        Vector<Object> updatePage = p.addRow(v, index);


                        pagesInfo.remove(i);
                        pagesInfo.add(i, updatePage);
                        System.out.println("inserted here!!!" + 1 + " " + pagesID.get(i) + " " + "and page count is" + " " + countRows + " " + v.get(index) + " " + max);

                        // sort in the vector
                        p.sortI(index);
                        DBApp.serialize(p, tableName + "-" + pagesID.get(i));

                        break;
                        // In case we want to refer back to binary search in the page itself:
                    } else {
                        // if pk is greater than max, add new page ( no overflow pages for the last page)
                        if (Page.compare(pk, max) > 0) {
                            addPage(v, index);

                            break;
                        }
                        // if pk is less than the max in the last page, create a new page
                        // and add the last row in the last page to the new page
                        // then insert into the last page ( not the new one)
                        else {
                            Page p = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(i));
                            Vector<Vector<Object>> rows_in_prevPage = p.getRows();
                            Vector<Object> to_be_shifted = rows_in_prevPage.get(p.getNumOfRows() - 1);
                            Vector<Vector<Object>> r = p.getRows();
                            r.remove(p.getNumOfRows() - 1);
                            p.setRows(r);
                            p.setNumOfRows(p.getNumOfRows() - 1);
                            p.setMax_pk_value(p.getRows().get(p.getNumOfRows() - 1).get(index));
                            // update the info in the original page as well (because it has been incremented but not decremented)
                            Vector<Object> updatePage = p.addRow(v, index);

                            updatePage.set(0, (Integer) (updatePage.get(0)));
                            updatePage.set(1, (p.getMin_pk_value()));
                            updatePage.set(2, (p.getMax_pk_value()));

                            pagesInfo.remove(i);
                            pagesInfo.add(i, updatePage);
                            System.out.println("inserted here!!!" + 2 + " " + pagesID.get(i) + "and page count is" + " " + countRows);
                            p.sortI(index);
                            addPage(to_be_shifted, index);
                            for (int k = 0; k < pagesID.size(); k++) {
                                System.out.print("All the pages available" + " " + pagesID.get(k));
                                System.out.println();
                            }

                            DBApp.serialize(p, tableName + "-" + pagesID.get(i));
                            break;
                        }
                    }

                }


                // check if the required page has an overflow page,
                // if so, insert into the overflow page if this overflow page has space,
                // otherwise create a new overflow page
                // if (Page.compare(max, pk) == 1 ) {
                if (Page.compare(max, pk) >= 0) {
                    System.out.println(max + " " + pk);
                    Page p = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(i));
                    if (countRows >= maxRows) {

                        if (p.getOverFlowInfo().size() != 0) {

                            boolean found_space = false;
                            Vector<Vector<Object>> overflowPagesInfo = p.getOverFlowInfo();
                            for (int j = 0; j < p.getOverFlowInfo().size(); j++) {
                                Vector<Object> overflow = overflowPagesInfo.get(j);
                                int ID = (int) overflow.get(0);
                                int numOfRowsInOverflow = (int) overflow.get(1);
                                Page o = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(i) + "." + ID);
                                if (o.getNumOfRows() < maxRows) {
                                    o.addOverflowRow(v);
                                    if (Trial.compare(p.getMin_pk_value(), pk) > 0) {
                                        System.out.println("IT ENTERS HERE");
                                        p.setMin_pk_value(pk);
                                        pagesInfo.get(i).set(1, pk);
                                    }
                                    o.sortI(index);

                                    if (ID == 2) System.out.println("OVERFLOW PAGE");
                                    System.out.println("inserted here!!!" + 4 + " " + pagesID.get(i) + "and page count is" + " " + countRows + " " + v.get(index) + " " + max);
                                    Vector<Vector<Object>> updatedOverflowInfoPages = updatePageOverflowInfo(p, o, j);
                                    p.setOverFlowInfo(updatedOverflowInfoPages);
                                    DBApp.serialize(o, tableName + "-" + pagesID.get(i) + "." + (ID + ""));
                                    found_space = true;
                                    break;
                                }
                                DBApp.serialize(o, tableName + "-" + pagesID.get(i) + "." + (ID + ""));
                            }
                            if (!found_space) {

                                System.out.println("OVERFLOW PAGE");
                                p.addOverflow(tableName, pagesID.get(i), v, index);
                                if (Trial.compare(p.getMin_pk_value(), pk) > 0) {
                                    p.setMin_pk_value(pk);
                                    pagesInfo.get(i).set(1, pk);
                                }
                                System.out.println("inserted here!!!" + 5 + " " + pagesID.get(i) + "and page count is" + " " + countRows + " " + v.get(index) + " " + max);

                            }
                            DBApp.serialize(p, tableName + "-" + (pagesID.get(i) + ""));
                            break;
                        }

                    /* if the main page we wanted to insert the new row in is full and does not have an overflow page,
                    check the following page. If the following page has room, insert in the following page, otherwise,
                    create a new overflow page linked to the main page. */
                        else if ((i + 1) < pagesID.size()) {
                            Vector<Object> page2 = pagesInfo.get(i + 1);
                            int countRows2 = (int) page2.get(0); // <"2,0,10000", >
                            Object min2 = page2.get(1);
                            Object max2 = page2.get(2);
                            // checking for overflow
                            if (countRows2 < maxRows) { //to think
                                Page p2 = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(i + 1));
                                Vector<Vector<Object>> rows_in_prevPage = p.getRows();
                                Vector<Object> to_be_shifted = rows_in_prevPage.get(p.getNumOfRows() - 1);

                                System.out.println("inserted here!!!" + 6 + " " + pagesID.get(i) + "and page count is" + " " + countRows + " " + v.get(index) + " " + max);
                                Vector<Vector<Object>> r = p.getRows();
                                r.remove(p.getNumOfRows() - 1);
                                p.setRows(r);
                                p.setNumOfRows(p.getNumOfRows() - 1);
                                p.setMax_pk_value(p.getRows().get(p.getNumOfRows() - 1).get(index));

                                Vector<Object> updatePage = p.addRow(v, index);
                                // I need to compare with the new ( after removing the last row)

                                updatePage.set(0, (Integer) (updatePage.get(0)));
                                updatePage.set(1, (p.getMin_pk_value()));
                                updatePage.set(2, (p.getMax_pk_value()));

                                System.out.println("inserted here!!!" + 7 + " " + pagesID.get(i) + "and page count is" + " " + countRows + " " + v.get(index) + " " + max);
                                pagesInfo.remove(i);
                                pagesInfo.add(i, updatePage);
                                //pagesInfo.setElementAt();

                                Vector<Object> updatePage2 = p2.addRow(to_be_shifted, index);
                                // already done in addrow
//                                // I need to compare with the added row

                                updatePage2.set(0, (Integer) (updatePage2.get(0)));
                                updatePage2.set(1, (p2.getMin_pk_value()));
                                updatePage2.set(2, (p2.getMax_pk_value()));
                                pagesInfo.remove(i + 1);
                                pagesInfo.add(i + 1, updatePage2);
                                p.sortI(index);
                                p2.sortI(index);
                                DBApp.serialize(p2, tableName + "-" + pagesID.get(i + 1));
                                DBApp.serialize(p, tableName + "-" + pagesID.get(i));
                                break;
                            } else {
                                //create an overflow page and insert into the new page
//                                System.out.println("OVERFLOW PAGE");
                                p.addOverflow(tableName, pagesID.get(i), v, index);
                                System.out.println("inserted here!!!" + 8 + " " + pagesID.get(i) + "and page count is" + " " + countRows + " " + v.get(index) + " " + max);
                                DBApp.serialize(p, tableName + "-" + (pagesID.get(i) + ""));
                                break;
                            }
                        }

                        System.out.println("Danger Zone1");
                    } else if (countRows < maxRows) {

                        // the pk lies within the range and there is room for it, so we add immediately into the page
                        Vector<Object> updatePage = p.addRow(v, index);
                        pagesInfo.remove(i);
                        pagesInfo.add(i, updatePage);
                        System.out.println("inserted here!!!" + 9 + " " + pagesID.get(i) + "and page count is" + " " + countRows + " " + v.get(index) + " " + max);
                        p.sortI(index);
                        DBApp.serialize(p, tableName + "-" + pagesID.get(i));
                        break;
                    }
                    System.out.println("break here");
                    break;
                } else if (countRows < maxRows) {
                    //   {
                    // pk greater than the max but there is room in the page.
                    Page p = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(i));
                    Vector<Object> updatePage = p.addRow(v, index);
                    pagesInfo.remove(i);
                    pagesInfo.add(i, updatePage);
                    System.out.println("inserted here!!!" + 10 + " " + pagesID.get(i) + "and page count is" + " " + countRows + " " + v.get(index) + " " + max);
                    p.sortI(index);
                    DBApp.serialize(p, tableName + "-" + pagesID.get(i));
                    break;
                }

            }
        }
    }
    /* 1-deserialize the grid
    2-call findcell */

    public void insertIntoGrid(Hashtable<String, Object> columnNameValues, String pageName, Object pk) {
        for (int i = 0; i < gridIndices.size(); i++) {
            GridIndex G = (GridIndex) DBApp.deserialize(tableName + "-GI" + i);
            String BucketName = G.findCell(columnNameValues);
            System.out.println("ARRAY PRINTER HERE");
            System.out.println(Arrays.toString(G.getColNames()));
            Bucket B;
            // check if the bucket already exists or not

            if (G.getBucketsinTable().contains(BucketName))
                B = (Bucket) DBApp.deserialize(BucketName);
            else
                B = G.addBucket(BucketName);

            B.insertIntoBucket(pk, pageName, columnNameValues, G);
            DBApp.serialize(B, BucketName); // Bucket
            DBApp.serialize(G, tableName + "-GI" + i); // Grid
        }

    }

    // the new address , row ,pk
    public void updateBucketAfterShiftingInInsert(Vector<Object> row) throws DBAppException, IOException {
        // delete the row
        // insert
        Hashtable<String, Object> colNameValueDummy = new Hashtable<String, Object>();
        System.out.print("PRINT HEREE " + colNamesTable.size() + "," + row.size());
        for (int i = 0; i < colNamesTable.size(); i++) {
            colNameValueDummy.put(colNamesTable.get(i), row.get(i));
        }


        Vector<Object> pkIndex_pk_Value = new Vector<>();
        pkIndex_pk_Value.add(pk_index);
        pkIndex_pk_Value.add(row.get(pk_index));
        Vector<Vector> index_valueDummy = new Vector<>();
        index_valueDummy.add(pkIndex_pk_Value);

        deleteUsingIndex(colNameValueDummy, row.get(pk_index), pk_index, index_valueDummy);
        Hashtable<String, Object> colNameValue = new Hashtable<String, Object>();
        // <mai, 23>
        //<name, age>
        for (int i = 0; i < row.size(); i++) {
            colNameValue.put(colNamesTable.get(i), row.get(i));
        }
        insertIntoPageWithGI(row, pk_index, colNameValue);
        /*
        1-take the row
        2-search in all grids for this row
        3-update the reference name
        */
    }


    public void insertIntoPageWithGI(Vector<Object> v, int index, Hashtable<String, Object> colNameValues) throws DBAppException, IOException {
        checktablePage();

        // check if the page is full, if yes, go to the next page and check whether it has space
        // if the next page has space, insert into the second page and do the needed shifting
        // otherwise create an overflow in the initial page we wanted to insert in
        // update min/max if needed
        // insert into the right page then call sortI


        // get the clusteringKey ck of v
        Object pk = v.get(index);
        System.out.println("THE VALUE TO BE INSERTED IS " + pk);
        // check whether the table has pages
        // if there is no page, create a new one and insert
        // insert the info related to this page into the pagesInfo Vector
        if (count == 0) {
            addPage(v, index);
            System.out.println("the amazing searchKey " + " to insert: " + pk);
            if (pks.contains(pk))
                throw new DBAppException("You can not insert a duplicate key for a primary key.");
            else {
                pks.add(pk);
            }

        } else {

            // check if pk already exists in the table
            if (pks.contains(pk))
                throw new DBAppException("You can not insert a duplicate key for a primary key.");
            else {
                pks.add(pk);
            }

// 0-100(has no space) , 120-130(has space)
// I want to insert 30, but 30 should not go into 120-130!, thus I have to insert in the prev page(0-100)
// which means I need to shift one record from 0-100 to 120-130 and insert into 0-100
// however if 120-130 is also full, create an overflow page for 0-100
            System.out.println("Binary maxes");
            for (int m = 0; m < pagesID.size(); m++) {
                System.out.print(pagesInfo.get(m).get(2) + " ");
            }

            int searchPage = binarySearch(pagesInfo, 0, pagesInfo.size() - 1, pk);
            // check whether the pk fits within the ranges of min-max of any existing page
            int i = searchPage;
            while (true) {
                System.out.println(i);
                if (i == -1) {
                    System.out.println("ur i is -1 ");
                    i = pagesID.size() - 1;
                }
                System.out.println("the amazing searchKey " + i + " to insert: " + pk);
                // retrieve the info of the page we're standing at
                Vector<Object> page = pagesInfo.get(i);

                Enumeration enu = pagesInfo.get(i).elements();
                System.out.println("The enumeration of values are:");
                // Displaying the Enumeration
                while (enu.hasMoreElements()) {
                    System.out.println(enu.nextElement());
                }

                int countRows = (int) page.get(0); // <"2,0,10000", >
                Object min = page.get(1);
                Object max = page.get(2);

                // if we are on the last page in the table
                if (i == (pagesID.size() - 1)) {

                    // check whether we have room for a new row in the last page.If so, add the new row.
                    if (countRows < maxRows) {
                        Page p = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(i));
                        // after adding the new row, update the page info and add it into the pagesInfo Vector in the table.
                        Vector<Object> updatePage = p.addRow(v, index);


                        pagesInfo.remove(i);
                        pagesInfo.add(i, updatePage);
                        System.out.println("inserted here!!!" + 1 + " " + pagesID.get(i) + " " + "and page count is" + " " + countRows + " " + v.get(index) + " " + max);

                        // sort in the vector
                        p.sortI(index);
                        DBApp.serialize(p, tableName + "-" + pagesID.get(i));
                        insertIntoGrid(colNameValues, tableName + "-" + pagesID.get(i), pk);
                        break;
                        // In case we want to refer back to binary search in the page itself:
                    } else {
                        // if pk is greater than max, add new page ( no overflow pages for the last page)
                        if (Page.compare(pk, max) > 0) {
                            addPage(v, index);
                            insertIntoGrid(colNameValues, tableName + "-" + count, pk);
                            break;
                        }
                        // if pk is less than the max in the last page, create a new page
                        // and add the last row in the last page to the new page
                        // then insert into the last page ( not the new one)
                        else {
                            Page p = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(i));
                            Vector<Vector<Object>> rows_in_prevPage = p.getRows();
                            Vector<Object> to_be_shifted = rows_in_prevPage.get(p.getNumOfRows() - 1);
                            Vector<Vector<Object>> r = p.getRows();
                            r.remove(p.getNumOfRows() - 1);
                            p.setRows(r);
                            p.setNumOfRows(p.getNumOfRows() - 1);
                            p.setMax_pk_value(p.getRows().get(p.getNumOfRows() - 1).get(index));
                            // update the info in the original page as well (because it has been incremented but not decremented)
                            Vector<Object> updatePage = p.addRow(v, index);

                            updatePage.set(0, (Integer) (updatePage.get(0)));
                            updatePage.set(1, (p.getMin_pk_value()));
                            updatePage.set(2, (p.getMax_pk_value()));

                            pagesInfo.remove(i);
                            pagesInfo.add(i, updatePage);
                            System.out.println("inserted here!!!" + 2 + " " + pagesID.get(i) + "and page count is" + " " + countRows);
                            p.sortI(index);

                            addPage(to_be_shifted, index);
                            for (int k = 0; k < pagesID.size(); k++) {
                                System.out.print("All the pages available" + " " + pagesID.get(k));
                                System.out.println();
                            }
                            // SHIFTING HERE!
                            insertIntoGrid(colNameValues, tableName + "-" + pagesID.get(i), pk);
                            updateBucketAfterShiftingInInsert(to_be_shifted);

                            DBApp.serialize(p, tableName + "-" + pagesID.get(i));
                            break;
                        }
                    }

                }


                // check if the required page has an overflow page,
                // if so, insert into the overflow page if this overflow page has space,
                // otherwise create a new overflow page
                // if (Page.compare(max, pk) == 1 ) {
                if (Page.compare(max, pk) >= 0) {
                    System.out.println(max + " " + pk);
                    Page p = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(i));
                    if (countRows >= maxRows) {

                        if (p.getOverFlowInfo().size() != 0) {

                            boolean found_space = false;
                            Vector<Vector<Object>> overflowPagesInfo = p.getOverFlowInfo();
                            for (int j = 0; j < p.getOverFlowInfo().size(); j++) {
                                Vector<Object> overflow = overflowPagesInfo.get(j);
                                int ID = (int) overflow.get(0);
                                int numOfRowsInOverflow = (int) overflow.get(1);
                                Page o = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(i) + "." + ID);
                                if (o.getNumOfRows() < maxRows) {
                                    o.addOverflowRow(v);
                                    if (Trial.compare(p.getMin_pk_value(), pk) > 0) {
                                        System.out.println("IT ENTERS HERE");
                                        p.setMin_pk_value(pk);
                                        pagesInfo.get(i).set(1, pk);
                                    }
                                    o.sortI(index);

                                    if (ID == 2) System.out.println("OVERFLOW PAGE");
                                    System.out.println("inserted here!!!" + 4 + " " + pagesID.get(i) + "and page count is" + " " + countRows + " " + v.get(index) + " " + max);
                                    Vector<Vector<Object>> updatedOverflowInfoPages = updatePageOverflowInfo(p, o, j);
                                    p.setOverFlowInfo(updatedOverflowInfoPages);
                                    DBApp.serialize(o, tableName + "-" + pagesID.get(i) + "." + (ID + ""));
                                    found_space = true;
                                    insertIntoGrid(colNameValues, tableName + "-" + pagesID.get(i) + "." + (ID + ""), pk);
                                    break;
                                }
                                DBApp.serialize(o, tableName + "-" + pagesID.get(i) + "." + (ID + ""));
                            }
                            if (!found_space) {

                                System.out.println("OVERFLOW PAGE");
                                p.addOverflow(tableName, pagesID.get(i), v, index);
                                if (Trial.compare(p.getMin_pk_value(), pk) > 0) {
                                    p.setMin_pk_value(pk);
                                    pagesInfo.get(i).set(1, pk);
                                }
                                System.out.println("inserted here!!!" + 5 + " " + pagesID.get(i) + "and page count is" + " " + countRows + " " + v.get(index) + " " + max);
                                int size = p.getOverFlowInfo().size() - 1;
                                int ID = (int) p.getOverFlowInfo().get(size).get(0);
                                insertIntoGrid(colNameValues, tableName + "-" + pagesID.get(i) + "." + (ID + ""), pk);
                            }
                            DBApp.serialize(p, tableName + "-" + (pagesID.get(i) + ""));

                            break;
                        }

                    /* if the main page we wanted to insert the new row in is full and does not have an overflow page,
                    check the following page. If the following page has room, insert in the following page, otherwise,
                    create a new overflow page linked to the main page. */
                        else if ((i + 1) < pagesID.size()) {
                            Vector<Object> page2 = pagesInfo.get(i + 1);
                            int countRows2 = (int) page2.get(0); // <"2,0,10000", >
                            Object min2 = page2.get(1);
                            Object max2 = page2.get(2);
                            // checking for overflow
                            if (countRows2 < maxRows) { //to think
                                Page p2 = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(i + 1));
                                Vector<Vector<Object>> rows_in_prevPage = p.getRows();
                                Vector<Object> to_be_shifted = rows_in_prevPage.get(p.getNumOfRows() - 1);

                                System.out.println("inserted here!!!" + 6 + " " + pagesID.get(i) + "and page count is" + " " + countRows + " " + v.get(index) + " " + max);
                                Vector<Vector<Object>> r = p.getRows();
                                r.remove(p.getNumOfRows() - 1);
                                p.setRows(r);
                                p.setNumOfRows(p.getNumOfRows() - 1);
                                p.setMax_pk_value(p.getRows().get(p.getNumOfRows() - 1).get(index));

                                Vector<Object> updatePage = p.addRow(v, index);
                                // I need to compare with the new ( after removing the last row)

                                updatePage.set(0, (Integer) (updatePage.get(0)));
                                updatePage.set(1, (p.getMin_pk_value()));
                                updatePage.set(2, (p.getMax_pk_value()));

                                System.out.println("inserted here!!!" + 7 + " " + pagesID.get(i) + "and page count is" + " " + countRows + " " + v.get(index) + " " + max);
                                pagesInfo.remove(i);
                                pagesInfo.add(i, updatePage);
                                //pagesInfo.setElementAt();

                                Vector<Object> updatePage2 = p2.addRow(to_be_shifted, index);
                                // already done in addrow
//                                // I need to compare with the added row

                                updatePage2.set(0, (Integer) (updatePage2.get(0)));
                                updatePage2.set(1, (p2.getMin_pk_value()));
                                updatePage2.set(2, (p2.getMax_pk_value()));
                                pagesInfo.remove(i + 1);
                                pagesInfo.add(i + 1, updatePage2);
                                p.sortI(index);
                                p2.sortI(index);
                                DBApp.serialize(p2, tableName + "-" + pagesID.get(i + 1));
                                DBApp.serialize(p, tableName + "-" + pagesID.get(i));
                                // SHIFTING HERE
//                                updateBucketAfterShiftingInInsert(to_be_shifted);
                                insertIntoGrid(colNameValues, tableName + "-" + pagesID.get(i), pk);
                                updateBucketAfterShiftingInInsert(to_be_shifted);
                                break;
                            } else {
                                //create an overflow page and insert into the new page
//                                System.out.println("OVERFLOW PAGE");
                                p.addOverflow(tableName, pagesID.get(i), v, index);
                                System.out.println("inserted here!!!" + 8 + " " + pagesID.get(i) + "and page count is" + " " + countRows + " " + v.get(index) + " " + max);
                                DBApp.serialize(p, tableName + "-" + (pagesID.get(i) + ""));

                                int size = p.getOverFlowInfo().size() - 1;
                                int ID = (int) p.getOverFlowInfo().get(size).get(0);
                                insertIntoGrid(colNameValues, tableName + "-" + pagesID.get(i) + "." + (ID + ""), pk);
                                break;
                            }
                        }

                        System.out.println("Danger Zone1");
                    } else if (countRows < maxRows) {

                        // the pk lies within the range and there is room for it, so we add immediately into the page
                        Vector<Object> updatePage = p.addRow(v, index);
                        pagesInfo.remove(i);
                        pagesInfo.add(i, updatePage);
                        System.out.println("inserted here!!!" + 9 + " " + pagesID.get(i) + "and page count is" + " " + countRows + " " + v.get(index) + " " + max);
                        p.sortI(index);
                        DBApp.serialize(p, tableName + "-" + pagesID.get(i));

                        insertIntoGrid(colNameValues, tableName + "-" + pagesID.get(i), pk);
                        break;
                    }
                    System.out.println("break here");
                    break;
                } else if (countRows < maxRows) {
                    //   {
                    // pk greater than the max but there is room in the page.
                    Page p = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(i));
                    Vector<Object> updatePage = p.addRow(v, index);
                    pagesInfo.remove(i);
                    pagesInfo.add(i, updatePage);
                    System.out.println("inserted here!!!" + 10 + " " + pagesID.get(i) + "and page count is" + " " + countRows + " " + v.get(index) + " " + max);
                    p.sortI(index);
                    DBApp.serialize(p, tableName + "-" + pagesID.get(i));
                    insertIntoGrid(colNameValues, tableName + "-" + pagesID.get(i), pk);
                    break;
                }

            }
        }
    }

    public void addPage(Vector<Object> v, int index) throws DBAppException {
        count++;
        Page p = new Page();
        p.setPk_index(index);
        p.setMax_pk_value(v.get(index));
        p.setMin_pk_value(v.get(index));
        p.addRow(v, index);

        this.pagesID.add(count);
        Vector<Object> newPage = new Vector<Object>();
        newPage.add(1);
        newPage.add(v.get(index));
        newPage.add(v.get(index));
        pagesInfo.add(newPage);
        DBApp.serialize(p, tableName + "-" + count + "");
    }

    //search for the correct page to insert to
    public int binarySearch(Vector<Vector<Object>> arr, int l, int r, Object x) {
        if (r >= l) {
            int mid = l + (r - l) / 2;
            // Creating an empty enumeration to store
            Enumeration enu = arr.get(mid).elements();

            System.out.println("The enumeration of values are:");
            // Displaying the Enumeration
            while (enu.hasMoreElements()) {
                System.out.println(enu.nextElement());
            }
            // If the element is present at the
            // middle itself <numofPage, min, max>
            Comparable xnew = (Comparable) x;
            Comparable arr_mid_min = (Comparable) (arr.get(mid)).get(1);
            Comparable arr_mid_max = (Comparable) (arr.get(mid)).get(2);

            if ((Trial.compare(x, (arr.get(mid)).get(1)) >= 0 && Trial.compare((arr.get(mid)).get(2), x) >= 0) ||
                    (mid == 0 && Trial.compare((arr.get(mid)).get(2), x) >= 0) ||
                    (mid == arr.size() - 1))
            //|| (((mid < arr.size() - 1)  &&  (Trial.compare(arr.get(mid + 1).get(1), x) >= 0))))
            // || ((mid < arr.size() - 1) && ((Trial.compare(x, arr.get(mid).get(1)) >= 0) && (Trial.compare(arr.get(mid + 1).get(1), x) >= 0))))
            {
                return mid;
            }

            // If element is smaller than mid, then
            // it can only be present in left subarray
            if (Trial.compare(arr_mid_max, x) > 0)
                return binarySearch(arr, l, mid - 1, x);
            // Else the element can only be present
            // in right subarray
            return binarySearch(arr, mid + 1, r, x);
        }
        // We reach here when element is not present
        // in array
        return -1;
    }

    public void removePage(Page p, int deletePage) {
        pagesInfo.remove(deletePage);
        pagesID.remove(deletePage);
    }

    public GridIndex chooseIndex(String tableName, Hashtable<String, Object> colNameValue) {
        GridIndex G = null;
        int count = 0;
        int size = (int) 10e6; //1,000,000
        for (int i = 0; i < gridIndices.size(); i++) {
            GridIndex tempG = (GridIndex) DBApp.deserialize(tableName + "-GI" + i);
            String[] colNames = tempG.getColNames(); //colName of the Grid

            int tempSize = tempG.getColNames().length;
            int tempCount = 0;
            for (int j = 0; j < colNames.length; j++) {
                if (colNameValue.containsKey(colNames[j])) {
                    tempCount++;
                }
            }
            if (tempCount > count || (tempCount == count && tempSize < size)) {
                G = tempG;
                count = tempCount;
                size = G.getColNames().length;
            } else {
                DBApp.serialize(tempG, tableName + "-GI" + i);
            }
        }
        return G;
    }

    public void deleteUsingIndex(Hashtable<String, Object> colNameValue, Object pk_value, int pk_found, Vector<Vector> index_value) throws DBAppException, IOException {
        GridIndex G = chooseIndex(tableName, colNameValue);

        System.out.println("G INICES AND VALUES");
        System.out.println(Arrays.toString(G.getColNames()));
        for (int i = 0; i < G.getDimVals().length; i++) {
            System.out.println(G.getDimVals()[i].toString());
        }
        Enumeration<String> keys1 = colNameValue.keys();
        //iterate
        while (keys1.hasMoreElements()) {
            String k = keys1.nextElement();
            System.out.println(k + " okok " + colNameValue.get(k));
        }


        //get address in current bucket  <pk,pageName,colname1,colname2,...>
        //   if (I have pk in hashtable:) bs in current bucket and its overflows, save row in vector
        if (pk_value != null) { //if i have primary key
            //do BS ON BUCKET AND OVERFLOW
            String BucketName = G.findCell(colNameValue);

            Bucket B;
            if (G.getBucketsinTable().contains(BucketName))
                B = (Bucket) DBApp.deserialize(BucketName);
            else
                throw new DBAppException("Cannot find bucket");

            int addressIdxInBucket = B.binarySearch(pk_value);

            // check the overflows
            if (addressIdxInBucket == -1) { //if not found in bucket

                if (B.getOverflowBucketsInfo().size() > 0) {

                    for (int i = 0; i < B.getOverflowBucketsInfo().size(); i++) {
                        // get overflow bucket:
                        Vector<Object> v = B.getOverflowBucketsInfo().get(i); // name and num of entries
                        Bucket Overflow = (Bucket) DBApp.deserialize(v.get(0) + "");
                        int addressIdxInOv = Overflow.binarySearch(pk_value);

                        if (addressIdxInOv != -1) {
                            B.getAddresses().remove(addressIdxInBucket);
                            B.setAddresses(B.getAddresses());
                            Vector<Object> address = Overflow.getAddresses().get(addressIdxInOv); //row in bucket to vector
                            String PageName = (String) address.get(1);
                            Page p = (Page) DBApp.deserialize(PageName);
                            Vector<Object> row = p.deleteRowFromPageUsingIdxB(pk_found, pk_value, index_value);
                            updateTablePagesInfo(p, pk_value);
                            DBApp.serialize(p, PageName);

                            deleteRowfromIndices(tableName, row, G, pk_value);


                            DBApp.serialize(Overflow, v.get(0) + "");
                            break;
                        }
                        DBApp.serialize(Overflow, v.get(0) + "");
                    }

                }
            } // ensure whether it is in the bucket or not
            else if (addressIdxInBucket != -1) {
                Vector<Object> address = B.getAddresses().get(addressIdxInBucket); //row in bucket to vector
                String PageName = (String) address.get(1);
                Page p = (Page) DBApp.deserialize(PageName);
                //get row from page
                B.getAddresses().remove(addressIdxInBucket);
                B.setAddresses(B.getAddresses());
                Vector<Object> row = p.deleteRowFromPageUsingIdxB(pk_found, pk_value, index_value);
                updateTablePagesInfo(p, pk_value);
// zis part is important to check rows in page

                if (row == null) {
//                    Page p1 = (Page) DBApp.deserialize("students-2");
                    System.out.println("Printing all rows in page to check why it is null " + pk_value + PageName);
                    for (int i = 0; i < p.getRows().size(); i++) {
                        System.out.println(p.getRows().get(i).toString());
                    }
                    System.out.println("Printing all rows in bucket to check why it is null " + pk_value);
                    for (int i = 0; i < B.getAddresses().size(); i++) {
                        System.out.println(B.getAddresses().get(i).toString());
                    }


                }
                DBApp.serialize(p, PageName);

                //delete from all indices
                deleteRowfromIndices(tableName, row, G, pk_value);
            }

            DBApp.serialize(B, BucketName); // Bucket
            DBApp.serialize(G, tableName + "-GI" + G.getGridID()); // Grid

        }
        // delete from all bucket ( pk is no longer the condition)
        else {
                /*
                1- get all buckets satisfying the where condition
                for loop( each Bucket){
                get vector of vector of entries to be deleted

                 for loop ( vector of vector of entries){
                    delete from page + get row from the table
                    delete each row from all indixes; }
                }
                 */
            Vector<String> bNames = G.findAllBuckets(colNameValue);
            for (int i = 0; i < bNames.size(); i++) {
                Bucket B = (Bucket) DBApp.deserialize(bNames.get(i));
                // bucket -> addresses + colNames of GI
                // GI: < col1, col2, col3>
                // Bucket entry:  < pk, pageName, val1, val2, val3>
                // colNameValues: <<col1,A>,<col3,B>>


                // vector: < col1, col3>
                Vector<String> colNamesRequired = new Vector<String>();
                Enumeration<String> keys = colNameValue.keys();
                //iterate
                while (keys.hasMoreElements()) {
                    colNamesRequired.add(keys.nextElement());
                }

                deleteRowsInBuckets(G, colNamesRequired, colNameValue, B, pk_found, index_value);
                for (int n = 0; n < B.getOverflowBucketsInfo().size(); n++) {
                    Bucket O = (Bucket) DBApp.deserialize(B.getOverflowBucketsInfo().get(0) + "");
                    deleteRowsInBuckets(G, colNamesRequired, colNameValue, B, pk_found, index_value);
                }

            }


        }
    }


    public void deleteRowsInBuckets(GridIndex G, Vector<String> colNamesRequired, Hashtable<String, Object> colNameValue, Bucket B, int pk_found, Vector<Vector> index_value) throws DBAppException, IOException {
        Vector<Vector<Object>> entries_deleted = new Vector<Vector<Object>>();
        String[] GI_colName = G.getColNames();
        Vector<Vector<Object>> B_addresses = new Vector<Vector<Object>>();
        for (int o = 0; o < B_addresses.size(); o++) {
            // create new hash map using GI + Bucket entry -> <<col1, val1>,<col2, val2>,<col3,val3>>
            Hashtable<String, Object> hs = new Hashtable<String, Object>();
            for (int j = 2, k = 0; j < GI_colName.length; j++, k++) {
                hs.put(GI_colName[k], B_addresses.get(o).get(j));
            }


            // checking if the required columns(where cond.) have the same value in the input hashtable and in the bucket entry
            boolean found2 = true;
            for (int z = 0; z < colNamesRequired.size(); z++) {
                if (Trial.compare(hs.get(colNamesRequired.get(z)), colNameValue.get(colNamesRequired.get(z))) != 0) {
                    found2 = false;
                    break;
                }
            }
            // add the rows to be deleted in the big vector.
            if (found2) {
                entries_deleted.add(B_addresses.get(o));
                B_addresses.remove(B_addresses.get(o));
            }
        }

        for (int p = 0; p < entries_deleted.size(); p++) {
            deleteRowFromTableAndIndices(entries_deleted.get(p), B, G, tableName, pk_found, index_value);
        }
        DBApp.serialize(B, B.getBucketName());
    }

    public void deleteRowFromTableAndIndices(Vector<Object> a, Bucket B, GridIndex G, String tableName, int pk_found, Vector<Vector> index_value) throws DBAppException, IOException {

        Vector<Object> address = a; //row in bucket to vector
        String PageName = (String) address.get(1);
        Page p = (Page) DBApp.deserialize(PageName);
        //get row from page
        Object pk_value = a.get(0);
        Vector<Object> row = p.deleteRowFromPageUsingIdxB(pk_found, pk_value, index_value);
        updateTablePagesInfo(p, pk_value);

        //delete from all indices
        deleteRowfromIndices(tableName, row, G, pk_value);

    }

    public void deleteRowfromIndices(String tableName, Vector<Object> row, GridIndex G, Object pk_value) throws DBAppException, IOException {
        for (int i = 0; i < gridIndices.size(); i++) {
            GridIndex tempG = G;
            Hashtable<String, Object> colNameValuesGrid = new Hashtable<String, Object>();

// this is used to construct the hashtable:
            String[] colNamesInGrid = G.getColNames(); //colName of the Grid


            for (int j = 0; j < colNamesTable.size(); j++) {
                for (int k = 0; k < colNamesInGrid.length; k++) {
                    if (colNamesTable.get(j).equals(colNamesInGrid[k])) {
                        System.out.println("ROW PRINTED HERE");
                        System.out.println(row.toString());
                        colNameValuesGrid.put(colNamesInGrid[k], row.get(j));
                        break;
                    }
                }
            }
            //CONTINUE HERERE
            Enumeration<String> keys1 = colNameValuesGrid.keys();
            //iterate
            while (keys1.hasMoreElements()) {
                String k = keys1.nextElement();
                System.out.println(k + " 1okok1 " + colNameValuesGrid.get(k));
            }
            System.out.println("grid printed columns " + Arrays.toString(G.getColNames()));
            String BucketName1 = tempG.findCell(colNameValuesGrid);

            //
            System.out.println("FFILE " + BucketName1);
            Bucket B1 = (Bucket) DBApp.deserialize(BucketName1);// pk, pagename, value1, value2, ...
            B1.deleteFromBucket(pk_value);
            // search within the bucket for these hashtable valeus and remove it from the bucket CONTINUE HERE
            // i need to call find cell, find cell takes parameters inside hashtable
        }

    }


    public void deleteFromPage(Vector<Vector> index_value, int pk_found, Object pk_value) throws DBAppException {
        //binary search
        int c = 0; // this is used to count the number of record deleted. ( the method delete returns boolean, so we count how many trues we have)

        if (pk_value != null) {
//            System.out.println("CHECKING PAGE INFO SIZE!"+sear);
            int searchPage = binarySearch(pagesInfo, 0, pagesInfo.size() - 1, pk_value);
            Page p = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(searchPage));
            boolean t = p.deleteRowFromPageB(pk_found, pk_value, index_value);
            if (t) {
                pks.remove(pk_value);
                c = c + 1;
                Vector<Object> updatePageInfo = new Vector<Object>();
                updatePageInfo.add(p.getNumOfRows());
                updatePageInfo.add(p.getMin_pk_value());
                updatePageInfo.add(p.getMax_pk_value());
                pagesInfo.set(searchPage, updatePageInfo);
            }
            if (p.getNumOfRows() == 0) {
                removePage(p, searchPage);
            } else {
                DBApp.serialize(p, tableName + "-" + pagesID.get(searchPage));
            }
            if (p.getOverFlowInfo().size() == 0) {
                if (c == 0) {
                } //throw new DBAppException("No such record.");
            } else
                deleteFromOverflowPage(searchPage, index_value, pk_found, pk_value, c);

        }
        // linear search
        else {
            for (int i = 0; i < pagesID.size(); i++) {
                Page p = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(i));
                Vector<Object> t = p.deleteRowFromPageL(pk_found, index_value);
                if (p.getNumOfRows() == 0) {
                    c++;
                    removePage(p, i);
                    i--;
                } else {
                    if (t.size() > 0) {
                        for (int g = t.size() - 1; g >= 0; g--) {
                            pks.remove(t.get(g));
                        }
                        c = c + 1;
                        Vector<Object> updatePageInfo = new Vector<Object>();
                        updatePageInfo.add(p.getNumOfRows());
                        updatePageInfo.add(p.getMin_pk_value());
                        updatePageInfo.add(p.getMax_pk_value());
                        pagesInfo.set(i, updatePageInfo);
                    }
                    DBApp.serialize(p, tableName + "-" + pagesID.get(i));
                }
                if (p.getOverFlowInfo().size() == 0) {
                    if (c == 0) {
                    } //throw new DBAppException("No such record.");
                } else
                    deleteFromOverflowPage(i, index_value, pk_found, pk_value, c);
            }


        }


    }


    public void deleteFromPageUsingIdx() {
    }

    public void deleteFromOverflowPage(int pageID, Vector<Vector> index_value, int pk_found, Object pk_value, int c) throws DBAppException {
        //   int c = 0;
        if (pk_value != null) {
            Page p = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(pageID));
            Vector<Vector<Object>> overflowPages = p.getOverFlowInfo();
            if (overflowPages != null) {
                for (int i = 0; i < overflowPages.size(); i++) {
                    Page o = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(pageID) + "." + overflowPages.get(i).get(0));
                    if (pk_value != null) {
                        boolean t = o.deleteRowFromPageB(pk_found, pk_value, index_value);
                        if (t) {
                            pks.remove(pk_value);
                            overflowPages.get(i).set(1, o.getNumOfRows());
                            c++;
                        }
                    } else {
                        Vector<Object> t = o.deleteRowFromPageL(pk_found, index_value);
                        if (t.size() > 0) {
                            for (int g = t.size() - 1; g >= 0; g--) {
                                pks.remove(t.get(g));
                            }

                            overflowPages.get(i).set(1, o.getNumOfRows());
                            c++;
                        }
                    }
                    if (o.getNumOfRows() == 0) {
                        overflowPages.remove(i);
                        i--;
                    } else {
                        DBApp.serialize(o, tableName + "-" + pagesID.get(pageID) + "." + overflowPages.get(i).get(0));
                    }
                }
                DBApp.serialize(p, tableName + "-" + pagesID.get(pageID));
            }
            if (c == 0) {
            }
        }
    }


    public void updateTablePagesInfo(Page p, Object pk_value) {
        int searchPage = binarySearch(pagesInfo, 0, pagesInfo.size() - 1, pk_value);
        pks.remove(pk_value);
        Vector<Object> updatePageInfo = new Vector<Object>();
        updatePageInfo.add(p.getNumOfRows());
        updatePageInfo.add(p.getMin_pk_value());
        updatePageInfo.add(p.getMax_pk_value());
        // how do we get the index of the page we want to delete
        pagesInfo.set(searchPage, updatePageInfo);

        if (p.getNumOfRows() == 0) {
            removePage(p, searchPage);
        } else {
            DBApp.serialize(p, tableName + "-" + pagesID.get(searchPage));
        }
    }


    public void updateInPage(Vector<Vector> index_value, int pk_found, Object pk_value) throws DBAppException {
        /*
        1- binary search on the page
        2- binary search on the pk_value
        3- update
         */

        int searchPage = binarySearch(pagesInfo, 0, pagesInfo.size() - 1, pk_value);
        if (searchPage == -1) throw new DBAppException("There is no existing record with the entered key value.");
        Page p = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(searchPage));
        boolean found = p.updateRowInPageB(pk_found, pk_value, index_value);
        DBApp.serialize(p, tableName + "-" + pagesID.get(searchPage));
        if (!found)
            updateInOverflowPage(searchPage, index_value, pk_found, pk_value);

    }

    private void updateInOverflowPage(Integer pageID, Vector<Vector> index_value, int pk_found, Object pk_value) throws DBAppException {
        if (pk_value != null) {
            Page p = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(pageID));
            System.out.println("UPDATED OVERFLOW");
            Vector<Vector<Object>> overflowPages = p.getOverFlowInfo();
            System.out.println(p.getOverFlowInfo().size());
            if (overflowPages.size() != 0) {
                System.out.println("UPDATED OVERFLOW1");
                int countup = 0;
                for (int i = 0; i < overflowPages.size(); i++) {
                    Page o = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(pageID) + "." + overflowPages.get(i).get(0));
                    boolean found = o.updateRowInPageB(pk_found, pk_value, index_value);
                    DBApp.serialize(o, tableName + "-" + pagesID.get(pageID) + "." + overflowPages.get(i).get(0));
                    if (found) {
                        countup++;
                        break;
                    }
                }
                DBApp.serialize(p, tableName + "-" + pagesID.get(pageID));
                if (countup == 0) throw new DBAppException("pk does not exist.");
            }
        }
    }

    public boolean isHasGrid() {
        return hasGrid;
    }

    public void setHasGrid(boolean hasGrid) {
        this.hasGrid = hasGrid;
    }

    public ArrayList<String> getColNamesTable() {
        return colNamesTable;
    }

    public void setColNamesTable(ArrayList<String> colNamesTable) {
        this.colNamesTable = colNamesTable;
    }

    public void updateInPagewithIndex(Hashtable<String, Object> colNameValue, Object pk_value, int pk_found, Vector<Vector> index_value) throws DBAppException, IOException {


        GridIndex G = chooseIndex(tableName, colNameValue);
        String BucketName = G.findCell(colNameValue);

        Bucket B;


        if (G.getBucketsinTable().contains(BucketName))
            B = (Bucket) DBApp.deserialize(BucketName);
        else
            throw new DBAppException("Cannot find bucket");

        int addressIdxInBucket = B.binarySearch(pk_value);

        // check the overflows
        if (addressIdxInBucket == -1) { //if not found in bucket

            if (B.getOverflowBucketsInfo().size() > 0) {

                for (int i = 0; i < B.getOverflowBucketsInfo().size(); i++) {
                    // get overflow bucket:
                    Vector<Object> v = B.getOverflowBucketsInfo().get(i); // name and num of entries
                    Bucket Overflow = (Bucket) DBApp.deserialize(v.get(0) + "");
                    int addressIdxInOv = Overflow.binarySearch(pk_value);
                    if (addressIdxInOv != -1) {

                        Vector<Object> address = Overflow.getAddresses().get(addressIdxInOv); //row in bucket to vector
                        String PageName = (String) address.get(1);
                        Page p = (Page) DBApp.deserialize(PageName);
                        Vector<Object> row = p.deleteRowFromPageUsingIdxB(pk_found, pk_value, index_value);
                        updateTablePagesInfo(p, pk_value);
                        deleteRowfromIndices(tableName, row, G, pk_value);
                        DBApp.serialize(p, PageName);


                        //update row
                        for (int j = 0; j < index_value.size(); j++) { //<<1,Ahmad>,<2,16>>
                            int rowToUpdateIndex = (int) index_value.get(j).get(0);
                            Object rowToUpdateValue = index_value.get(j).get(1);
                            row.set(rowToUpdateIndex, rowToUpdateValue);
                        }
                        DBApp.serialize(Overflow, v.get(0) + "");


                        B.getAddresses().remove(addressIdxInOv);
                        B.setAddresses(B.getAddresses());
                        //insert
                        insertIntoPageWithGI(row, pk_found, colNameValue);
                        //ser
                        DBApp.serialize(B, BucketName); // Bucket
                        DBApp.serialize(G, tableName + "-GI" + G.getGridID()); // Grid

                        break;
                    }
                    DBApp.serialize(Overflow, v.get(0) + "");
                    //ser
                    DBApp.serialize(B, BucketName); // Bucket
                    DBApp.serialize(G, tableName + "-GI" + G.getGridID()); // Grid
                }

            }
        }

        // ensure whether it is in the bucket or not
        else if (addressIdxInBucket != -1) {
            Vector<Object> address = B.getAddresses().get(addressIdxInBucket); //row in bucket to vector
            String PageName = (String) address.get(1);
            Page p = (Page) DBApp.deserialize(PageName);
            //get row from page
            Vector<Object> row = p.deleteRowFromPageUsingIdxB(pk_found, pk_value, index_value);
            updateTablePagesInfo(p, pk_value);
            DBApp.serialize(p, PageName);
            //delete from all indices
            deleteRowfromIndices(tableName, row, G, pk_value);

            //update row
            for (int j = 0; j < index_value.size(); j++) { //<<1,Ahmad>,<2,16>>
                int rowToUpdateIndex = (int) index_value.get(j).get(0);
                Object rowToUpdateValue = index_value.get(j).get(1);
                row.set(rowToUpdateIndex, rowToUpdateValue);
            }

            B.getAddresses().remove(addressIdxInBucket);
            B.setAddresses(B.getAddresses());

            //insert
            insertIntoPageWithGI(row, pk_found, colNameValue);

            //ser
            DBApp.serialize(B, BucketName); // Bucket
            DBApp.serialize(G, tableName + "-GI" + G.getGridID()); // Grid


        }


    }


    public void rehomeAlreadyMadeRows(GridIndex GI){

        for (int i = 0; i < (pagesID).size(); i++) {
            Vector<Object> page = pagesInfo.get(i);
            Page p = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(i));
            Vector<Vector<Object>> rows = p.getRows();
            // rows of page:
            for (int j = 0; j < rows.size(); j++) { // rows in page
                Vector<Object> row = rows.get(j);

                Hashtable<String, Object>colNameValues = new Hashtable<>();

                for (int k = 0; k < GI.getColNames().length; k++) {

                    String key = GI.getColNames()[k];
                    for(int e =0; e<colNamesTable.size();e++){

                        if(key.equals(colNamesTable.get(e))){
                            colNameValues.put(key,row.get(e));
                            break;
                        }
                    }


                }
                String BucketName = GI.findCell(colNameValues);
                Bucket B;
                // check if the bucket already exists or not

                if (GI.getBucketsinTable().contains(BucketName))
                    B = (Bucket) DBApp.deserialize(BucketName);
                else
                    B = GI.addBucket(BucketName);

                B.insertIntoBucket(row.get(pk_index), tableName + "-" + pagesID.get(i), colNameValues, GI);
                DBApp.serialize(B, BucketName); // Bucket

                DBApp.serialize(p,tableName + "-" + pagesID.get(i));




            }

            // check if this page has overflows:
            if (p.getOverFlowInfo().size() != 0) {
                Vector<Vector<Object>> overflowPagesInfo = p.getOverFlowInfo();
                for (int k = 0; k < p.getOverFlowInfo().size(); k++) {
                    Vector<Object> overflow = overflowPagesInfo.get(k);
                    int ID = (int) overflow.get(0);
                    Page o = (Page) DBApp.deserialize(tableName + "-" + pagesID.get(i) + "." + ID);
                    Vector<Vector<Object>> rowsOverflow = o.getRows();

                    for (int l = 0; l < o.getRows().size(); l++) {

                        Vector<Object> row = o.getRows().get(l);

                        Hashtable<String, Object>colNameValues = new Hashtable<>();

                        for (int r = 0; r < GI.getColNames().length; r++) {

                            String key = GI.getColNames()[r];
                            for(int e =0; e<colNamesTable.size();e++){

                                if(key.equals(colNamesTable.get(e))){
                                    colNameValues.put(key,row.get(e));
                                    break;
                                }
                            }


                        }
                        String BucketName = GI.findCell(colNameValues);
                        Bucket B;
                        // check if the bucket already exists or not

                        if (GI.getBucketsinTable().contains(BucketName))
                            B = (Bucket) DBApp.deserialize(BucketName);
                        else
                            B = GI.addBucket(BucketName);

                        B.insertIntoBucket(row.get(pk_index), tableName + "-" + pagesID.get(i) + "." + ID, colNameValues, GI);
                        DBApp.serialize(B, BucketName); // Bucket
                        DBApp.serialize(o,tableName + "-" + pagesID.get(i) + "." + ID);




                    }
                }
            }
        }

    }

}