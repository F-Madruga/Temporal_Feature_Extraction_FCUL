package featureextraction;

public class Main {
    public static void main(String[] args) {
        DataFrame df = DataFrame.readCSV("./dataset.csv", ",", false);
        df.setColumnsNames(new String[] {"INDEX", "YEAR", "MONTH", "DAY", "AMOUNT", "BUYER", "SELLER", "CREDIT_CARD", "LATITUDE", "LONGITUDE"});
        // System.out.println(df);
        long start = System.currentTimeMillis();
        // System.out.println(df.sum());
        System.out.println(df.min(new String[] {"INDEX", "YEAR", "MONTH", "DAY", "AMOUNT", "BUYER", "SELLER", "CREDIT_CARD", "LATITUDE", "LONGITUDE"}));
        System.out.println("Time elapsed: " + (System.currentTimeMillis() - start));
    }
}
