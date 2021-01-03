package featureextraction;

public class Main {
    public static void main(String[] args) {
        DataFrame df = DataFrame.readCSV("./dataset.csv", ",", false);
        df.setColumnsNames(new String[] {"INDEX", "YEAR", "MONTH", "DAY", "AMOUNT", "BUYER", "SELLER", "CREDIT_CARD", "LATITUDE", "LONGITUDE"});
        // System.out.println(df);
        long start = System.currentTimeMillis();
        // System.out.println(df.sum());
        System.out.println(df.filter((columns, row) -> {
            return (Integer) row.get(columns.get("SELLER")) == 302 && (Integer) row.get(columns.get("YEAR")) == 2018 && (Integer) row.get(columns.get("MONTH")) == 10;
        }).mean(new String[]  {"AMOUNT"}));
        System.out.println(df.filter((columns, row) -> {
            return (Integer) row.get(columns.get("BUYER")) == 18;
        }).mean(new String[]  {"AMOUNT"}));
        //System.out.println(df.);
        System.out.println("Time elapsed: " + (System.currentTimeMillis() - start));
    }
}
