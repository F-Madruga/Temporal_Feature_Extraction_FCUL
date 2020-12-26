package featureextraction;

public class Main {
    public static void main(String[] args) {
        DataFrame df = DataFrame.readCSV("./dataset.csv", ",", false);
        df.setColumnsNames(new String[] {"INDEX", "YEAR", "MONTH", "DAY", "AMOUNT", "BUYER", "SELLER", "CREDIT_CARD", "LATITUDE", "LONGITUDE"});
        System.out.println(df);
    }
}
