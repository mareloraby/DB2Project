import java.time.LocalDate;
import java.util.*;

public class Trial {


    public static void sortI(Vector<Vector> v, int index) {
        Vector<Object> pks = new Vector<>();
        for (int i = 0; i < v.size(); i++) {
            Vector row = v.get(i);
            pks.add(row.get(index));
        }

        for (int i = 1; i < pks.size(); i++) {
            // da in case pks.size()= v.size() => insertion sort
            Object value = pks.get(i);
            Vector<Object> o = v.get(i);
            int j;
            for (j = i - 1; j >= 0 && ((compare(pks.get(j), value) > 0)); j--) {
                pks.setElementAt(pks.get(j), j + 1);
                v.setElementAt(v.get(j), j + 1);
            }
            pks.setElementAt(value, j + 1);
            v.setElementAt(o, j + 1);
        }


    }

    public static void vToString(Vector<Object> v) // print contents of vector of objects
    {
        for (int i = 0; i < v.size(); i++) {
            if (v.get(i) instanceof String) {
                System.out.print((String) v.get(i) + " ");
            } else {
                if (v.get(i) instanceof Character) {
                    System.out.print((Character) v.get(i) + " ");
                } else {
                    if (v.get(i) instanceof Integer) {
                        System.out.print((Integer) v.get(i) + " ");
                    } else {
                        if (v.get(i) instanceof Boolean)//doesn't make much sense but just in case
                        {
                            System.out.print((Boolean) v.get(i) + " ");
                        } else {
                            if (v.get(i) instanceof Double) {
                                System.out.print((Double) v.get(i) + " ");
                            } else {
                                if (v.get(i) instanceof Float) {
                                    System.out.print((Float) v.get(i) + " ");
                                }
                            }
                        }
                    }
                }
            }
        }

    }




    public static int compare(Object o1, Object o2) {

        return (((Comparable) o1).compareTo((Comparable) o2));
    }

//    public static int compare(Object o1, Object o2) { // compares 2 objects
//
//        if (o1 instanceof Date && o2 instanceof Date) {
//
//            if (((Date) o1).compareTo((Date) o2) > 0) return 1; // o1 appears after o2
//            else if (((Date) o1).compareTo((Date) o2) < 0) return -1;
//            else return 0;
//
//        }
//        if (o1 instanceof Double && o2 instanceof Double) {
//            if ((Double) o1 < (Double) o2) {
//                return -1;
//            } else {
//                if ((Double) o1 > (Double) o2) {
//                    return 1;
//                } else {
//                    return 0;
//                }
//            }
//        }
//        if (o1 instanceof Integer && o2 instanceof Integer) {
//            if ((Integer) o1 < (Integer) o2) {
//                return -1;
//            } else {
//                if ((Integer) o1 > (Integer) o2) {
//                    return 1;
//                } else {
//                    return 0;
//                }
//            }
//        }
//        if (o1 instanceof Float && o2 instanceof Float) {
//            if ((Float) o1 < (Float) o2) {
//                return -1;
//            } else {
//                if ((Float) o1 > (Float) o2) {
//                    return 1;
//                } else {
//                    return 0;
//                }
//            }
//        } else {
//            if (o1 instanceof Boolean && o2 instanceof Boolean) {
//                return Boolean.compare((Boolean) o1, (Boolean) o2);
//            } else {
//                if (o1 instanceof Character && o2 instanceof Character) {
//                    return Character.compare((Character) o1, (Character) o2);
//                } else {
//                    if (o1 instanceof String && o2 instanceof String) {
//                        return ((String) o1).compareTo((String) o2);
//                    } else {
//                        return 30;
//                    }
//                }
//            }
//        }
//    }




    public static void toStringV(Vector<Vector> v) // prints vector of vectors
    {
        for (int i = 0; i < v.size(); i++) {
            for (int j = 0; j < v.get(i).size(); j++) {
                if (v.get(i).get(j) instanceof Integer) {
                    System.out.print((Integer) v.get(i).get(j) + " ");
                } else {
                    if (v.get(i).get(j) instanceof Double) {
                        System.out.print((Double) v.get(i).get(j) + " ");
                    } else {
                        if (v.get(i).get(j) instanceof Float) {
                            System.out.print((Float) v.get(i).get(j) + " ");
                        } else {
                            if (v.get(i).get(j) instanceof String) {
                                System.out.print((String) v.get(i).get(j) + " ");
                            } else {
                                if (v.get(i).get(j) instanceof Character) {
                                    String l = (Character) v.get(i).get(j) + "";
                                    System.out.print(l + " ");
                                } else {
                                    if (v.get(i).get(j) instanceof Boolean) {
                                        System.out.print((Boolean) v.get(i).get(j) + " ");
                                    }
                                }
                            }
                        }
                    }
                }
            }
            System.out.println();
        }
    }

    public static void main(String[] args) // for testing
    {
        Vector<Vector> v = new Vector<Vector>(); //page
        Vector<Object> pks = new Vector<Object>(); // clustering keys

        Vector<Object> v1 = new Vector<Object>(); //row
        v1.add("cluster1");
        v1.add(60);
        v.add(v1);
        pks.add(60);

        Vector<Object> v2 = new Vector<Object>(); //row
        v2.add("cluster2");
        v2.add(100);
        v.add(v2);
        pks.add(100);

        Vector<Object> v3 = new Vector<Object>(); //row
        v3.add("cluster3");
        v3.add(-5);
        v.add(v3);
        pks.add(-5);

//		vToString(pks);
//		System.out.println();
//		sortI(v,pks);
//		vToString(pks);

        toStringV(v);
        System.out.println();
        //vToString(pks);
        System.out.println();
//		sortI(v,pks);
        toStringV(v);


//		Vector<Integer> Vint = new Vector<Integer>();
//		Vint.add(5);
//		Vint.add(5);
//		Vint.add(4);
//		Vint.add(10);
//		Vint.add(3);
//		Vint.add(60);
//		Vint.add(8);
//		Vint.add(3);
//		Vint.add(-4);

        //sortI(Vint);
        //vToString(Vint);


    }


}
