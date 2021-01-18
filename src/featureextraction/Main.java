package featureextraction;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {
    public static void main(String[] args) {
        long totalStart = System.currentTimeMillis();
        long start = System.currentTimeMillis();

        String path = args.length > 0 ? args[0] : "./dataset.csv";
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

        // TASK 1
        start = System.currentTimeMillis();

        dataset = FeatureAggregator.doAggregated(dataset, new int[] {6}, ((groupIndexes, currentIndividual, dataset1) -> {
            if (groupIndexes.size() >= 1) {
                Integer lastIndex = groupIndexes.get(groupIndexes.size() - 1);
                List<Float> lastIndividual = dataset1.get(lastIndex);

                float sum = (lastIndividual.get(lastIndividual.size() - 1) * groupIndexes.size())+ currentIndividual.get(4);
                float mean = sum / (groupIndexes.size() + 1);
                currentIndividual.add(mean);
            }
            else {
                currentIndividual.add(currentIndividual.get(4));
            }
            return currentIndividual;
        }));

        System.out.println("Time elapsed on task 1: " + (System.currentTimeMillis() - start) + "\n");

        // TASK 2
        start = System.currentTimeMillis();

        dataset = FeatureAggregator.doAggregated(dataset, new int[] {5}, ((groupIndexes, currentIndividual, dataset1) -> {
            if (groupIndexes.size() >= 1) {
                Integer lastIndex = groupIndexes.get(groupIndexes.size() - 1);
                List<Float> lastIndividual = dataset1.get(lastIndex);
                float sum = (lastIndividual.get(lastIndividual.size() - 1) * groupIndexes.size())+ currentIndividual.get(4);
                float mean = sum / (groupIndexes.size() + 1);
                currentIndividual.add(mean);
            }
            else {
                currentIndividual.add(currentIndividual.get(4));
            }
            return currentIndividual;
        }));

        System.out.println("Time elapsed on task 2: " + (System.currentTimeMillis() - start) + "\n");

        // TASK 3
        start = System.currentTimeMillis();

        dataset = FeatureAggregator.doAggregated(dataset, new int[] {6, 1, 2}, ((groupIndexes, currentIndividual, dataset1) -> {
            if (groupIndexes.size() >= 1) {
                Integer lastIndex = groupIndexes.get(groupIndexes.size() - 1);
                List<Float> lastIndividual = dataset1.get(lastIndex);
                float sum = (lastIndividual.get(lastIndividual.size() - 1) * groupIndexes.size())+ currentIndividual.get(4);
                float mean = sum / (groupIndexes.size() + 1);
                currentIndividual.add(mean);
            }
            else {
                currentIndividual.add(currentIndividual.get(4));
            }
            return currentIndividual;
        }));

        System.out.println("Time elapsed on task 3: " + (System.currentTimeMillis() - start) + "\n");

        // TASK 4
        start = System.currentTimeMillis();

        dataset = FeatureAggregator.doAggregated(dataset, new int[] {6}, ((groupIndexes, currentIndividual, dataset1) -> {
            if (groupIndexes.size() >= 1 && groupIndexes.size() < 100) {
                Integer lastIndex = groupIndexes.get(groupIndexes.size() - 1);
                List<Float> lastIndividual = dataset1.get(lastIndex);

                float sum = (lastIndividual.get(lastIndividual.size() - 1) * groupIndexes.size()) + currentIndividual.get(4);
                float mean = sum / (groupIndexes.size() + 1);
                currentIndividual.add(mean);
            }
            else if (groupIndexes.size() >= 1) {
                Integer lastIndex = groupIndexes.get(groupIndexes.size() - 1);
                List<Float> lastIndividual = dataset1.get(lastIndex);

                int indexOfIndividual100 = groupIndexes.get(groupIndexes.size() - 100);
                List<Float> individual100 = dataset1.get(indexOfIndividual100);

                float sum = (lastIndividual.get(lastIndividual.size() - 1) * 100) - individual100.get(4) + currentIndividual.get(4);
                float mean = sum / 100;
                currentIndividual.add(mean);
            }
            else {
                currentIndividual.add(currentIndividual.get(4));
            }
            return currentIndividual;
        }));

        System.out.println("Time elapsed on task 4: " + (System.currentTimeMillis() - start) + "\n");

        // TASK 5
        start = System.currentTimeMillis();

        dataset = FeatureAggregator.doAggregated(dataset, new int[] {6}, ((groupIndexes, currentIndividual, dataset1) -> {
            if (groupIndexes.size() >= 1) {
                Integer lastIndex = groupIndexes.get(groupIndexes.size() - 1);
                List<Float> lastIndividual = dataset1.get(lastIndex);

                float sum = (lastIndividual.get(10) * groupIndexes.size()) + currentIndividual.get(4);
                float mean = sum / (groupIndexes.size() + 1);

                float standardDeviationSum = 0;
                for (Integer groupIndex : groupIndexes) {
                    standardDeviationSum += Math.pow(dataset1.get(groupIndex).get(4) - mean, 2);
                }

                standardDeviationSum += Math.pow(currentIndividual.get(4) - mean, 2);

                float standartDeviation = (float) Math.sqrt(standardDeviationSum/(groupIndexes.size() + 1));

                currentIndividual.add(currentIndividual.get(4) + (3 * standartDeviation));

            }
            else {
                currentIndividual.add(currentIndividual.get(4));
            }
            return currentIndividual;
        }));

        System.out.println("Time elapsed on task 5: " + (System.currentTimeMillis() - start) + "\n");

        start = System.currentTimeMillis();

        // TASK 6
        dataset = FeatureAggregator.doAggregated(dataset, new int[] {5}, ((groupIndexes, currentIndividual, dataset1) -> {
            if (groupIndexes.size() >= 1) {
                Integer lastIndex = groupIndexes.get(groupIndexes.size() - 1);
                List<Float> lastIndividual = dataset1.get(lastIndex);
                double distance = Math.sqrt(Math.pow(lastIndividual.get(8) - currentIndividual.get(8), 2) + Math.pow(lastIndividual.get(9) - currentIndividual.get(9), 2));
                currentIndividual.add((float) distance);
            }
            else {
                currentIndividual.add(0f);
            }
            return currentIndividual;
        }));

        System.out.println("Time elapsed on task 6: " + (System.currentTimeMillis() - start) + "\n");

        // TASK 7
        start = System.currentTimeMillis();

        dataset = FeatureAggregator.doAggregated(dataset, new int[] {7}, ((groupIndexes, currentIndividual, dataset1) -> {
            if (groupIndexes.size() >= 1) {
                Integer lastIndex = groupIndexes.get(groupIndexes.size() - 1);
                currentIndividual.add((float)(groupIndexes.size() + 1));
            }
            else {
                currentIndividual.add(1f);
            }
            return currentIndividual;
        }));

        System.out.println("Time elapsed on task 7: " + (System.currentTimeMillis() - start) + "\n");

        // TASK 8
        start = System.currentTimeMillis();

        dataset = FeatureAggregator.doAggregated(dataset, new int[] {5, 2}, ((groupIndexes, currentIndividual, dataset1) -> {
            if (groupIndexes.size() >= 1) {
                Integer lastIndex = groupIndexes.get(groupIndexes.size() - 1);
                List<Float> lastIndividual = dataset1.get(lastIndex);
                float sum = (lastIndividual.get(lastIndividual.size() - 1) * groupIndexes.size())+ currentIndividual.get(4);
                float mean = sum / (groupIndexes.size() + 1);
                currentIndividual.add(mean);
            }
            else {
                currentIndividual.add(currentIndividual.get(4));
            }
            return currentIndividual;
        }));

        System.out.println("Time elapsed on task 8: " + (System.currentTimeMillis() - start) + "\n");

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
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Time elapsed writing: " + (System.currentTimeMillis() - start) + "\n");

        System.out.println("Time elapsed: " + (System.currentTimeMillis() - totalStart));
    }
}
