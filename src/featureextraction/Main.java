package featureextraction;

public class Main {
    public static void main(String[] args) {
        long totalStart = System.currentTimeMillis();
        long start = System.currentTimeMillis();
        DataFrame df = DataFrame.readCSV("./dataset.csv", ",", false);
        df.setColumnsNames(new String[] {"INDEX", "YEAR", "MONTH", "DAY", "AMOUNT", "BUYER", "SELLER", "CREDIT_CARD", "LATITUDE", "LONGITUDE"});
        System.out.println("Time reading file: " + (System.currentTimeMillis() - start) + "\n");

        int seller = 302;
        int buyer = 18;
        int year = 2018;
        int month = 10;

        // Alinia 1
        start = System.currentTimeMillis();
        System.out.println("The average amount of all past purchases of the seller " + seller + " is " +
                        df.filter((columns, row) -> {
                            return (Integer) row.get(columns.get("SELLER")) == seller;
                        }).mean(new String[]  {"AMOUNT"}).getData().get(0).get(0)
                );
        System.out.println("Time elapsed: " + (System.currentTimeMillis() - start) + "\n");

        // Alinia 2
        start = System.currentTimeMillis();
        System.out.println("The average amount of all past purchases of the buyer " + buyer + " is " +
                        df.filter((columns, row) -> {
                            return (Integer) row.get(columns.get("BUYER")) == buyer;
                        }).mean(new String[]  {"AMOUNT"}).getData().get(0).get(0)
                );
        System.out.println("Time elapsed: " + (System.currentTimeMillis() - start) + "\n");

        // Alinia 3
        start = System.currentTimeMillis();
        System.out.println("The average amount of all past purchases of the seller " + seller + " in the month " + month + " of " + year + " is " +
                        df.filter((columns, row) -> {
                            return (Integer) row.get(columns.get("SELLER")) == seller && (Integer) row.get(columns.get("YEAR")) == year && (Integer) row.get(columns.get("MONTH")) == month;
                        }).mean(new String[]  {"AMOUNT"}).getData().get(0).get(0)
                );
        System.out.println("Time elapsed: " + (System.currentTimeMillis() - start) + "\n");

        System.out.println("Total time elapsed: " + (System.currentTimeMillis() - totalStart));
    }
}
