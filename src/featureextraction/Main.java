package featureextraction;

import java.util.List;

public class Main {
    public static void main(String[] args) {
        DataFrame df = DataFrame.readCSV("./dataset.csv", ",", false);
        df.setColumnsNames(new String[] {"INDEX", "YEAR", "MONTH", "DAY", "AMOUNT", "BUYER", "SELLER", "CREDIT_CARD", "LATITUDE", "LONGITUDE"});
        // System.out.println(df);
        long start = System.currentTimeMillis();
        System.out.println(df.sum(new String[] {"INDEX", "YEAR", "MONTH", "DAY", "AMOUNT", "BUYER", "SELLER", "CREDIT_CARD", "LATITUDE", "LONGITUDE"}));
        System.out.println("Time elapsed: " + (System.currentTimeMillis() - start));
        /*for (Object obj : data.get(0)) {
            System.out.println(obj);
            System.out.println(obj.getClass().getName());
        }*/
    }
}
