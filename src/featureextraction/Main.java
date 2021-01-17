package featureextraction;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {
    public static void main(String[] args) {
        /* long totalStart = System.currentTimeMillis();
        long start = System.currentTimeMillis();
        DataFrame2 df = DataFrame2.readCSV("./dataset.csv", ",", false);
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

        System.out.println("Total time elapsed: " + (System.currentTimeMillis() - totalStart)); */

        /*List<Map<String, List<Integer>>> subsets = FeatureAggregator.doAggregated(dataset, new int[] {5, 6}, ((groupIndexes, currentIndividual, dataset1, phase) -> {
            if (groupIndexes.size() >= 1) {
                Integer lastIndex = groupIndexes.get(groupIndexes.size() - 1);
                List<Float> lastIndividual = dataset.get(lastIndex);
                if (phase == 0) {
                    currentIndividual.add(lastIndividual.get(lastIndividual.size() - 1) + currentIndividual.get(4));
                }
                else {
                    currentIndividual.set(currentIndividual.size() - 1, lastIndividual.get(lastIndividual.size() - 1) + currentIndividual.get(currentIndividual.size() - 1));
                }
            }
            else {
                if (phase == 0) {
                    currentIndividual.add(currentIndividual.get(4));
                }
            }
            return currentIndividual;
        }));*/




        // System.out.println(subsets.get(1).get(group));
        /*List<Integer> individuals = new ArrayList<>();
        for (Map<String, List<Integer>> subset: subsets) {
            if (subset.containsKey(group)) {
                List<Integer> groupData = subset.get(group);
                individuals.addAll(groupData);
            }
        }*/
        /*
        System.out.println("Group = " + group);
        System.out.println("Size = " + subsets.get(1).get(group).size());
        System.out.println("Individual indexes = " + subsets.get(1).get(group));
         */

        long totalStart = System.currentTimeMillis();
        long start = System.currentTimeMillis();

        String path = "./dataset.csv";
        String separator = ",";

        List<List<Float>> dataset = new ArrayList<>();

        Map<String, Integer> columnsNames = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] rowData = line.split(separator);
                List<Float> individual = new ArrayList<>();
                for (String cellData : rowData) {
                    individual.add(Float.parseFloat(cellData));
                }
                dataset.add(individual);
            }
        } catch (IOException e) {
            System.out.println("Error reading CSV file");
            e.printStackTrace();
        }

        System.out.println("Time elapsed reading: " + (System.currentTimeMillis() - start) + "\n");

        start = System.currentTimeMillis();

        dataset = FeatureAggregator.doAggregated(dataset, new int[] {5, 6}, ((groupIndexes, currentIndividual, dataset1) -> {
            if (groupIndexes.size() >= 1) {
                Integer lastIndex = groupIndexes.get(groupIndexes.size() - 1);
                List<Float> lastIndividual = dataset1.get(lastIndex);
                currentIndividual.add(lastIndividual.get(lastIndividual.size() - 1) + currentIndividual.get(4));
            }
            else {
                currentIndividual.add(currentIndividual.get(4));
            }
            return currentIndividual;
        }));

        System.out.println("Time elapsed on task 1: " + (System.currentTimeMillis() - start) + "\n");





        start = System.currentTimeMillis();

        String outputPath = "dataset_out.csv";
        FileWriter writer = null;
        try {
            writer = new FileWriter(outputPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (List<Float> record: dataset) {
            String str = "";
            for (int i = 0; i < record.size(); i++) {
                str += record.get(i);
                if (i != record.size() - 1) {
                    str += ",";
                }
            }
            try {
                writer.write(str);
                writer.write(System.getProperty( "line.separator" ));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        // writer.flush(); // close() should take care of this
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();
        System.out.println("Time elapsed writing: " + (System.currentTimeMillis() - start) + "\n");

        System.out.println("Time elapsed: " + (System.currentTimeMillis() - totalStart));
    }
}
