package featureextraction;

import doinparallel.DoInParallelFrameWork;

import java.util.*;

public class FeatureAggregator {

    /* public static List<Map<String, List<Integer>>> doAggregated(List<List<Float>> dataset, int[] features, AggregatedWork aggregatedWork) {
        int defaultNThreads = Runtime.getRuntime().availableProcessors();
        return doAggregated(dataset, features, aggregatedWork, defaultNThreads);
    } */

    /*public static List<Map<String, List<Integer>>> doAggregated(List<List<Float>> dataset, int[] features, AggregatedWork aggregatedWork, int nthreads) {
        List<List<Map<String, List<Integer>>>> preSubsets = DoInParallelFrameWork.doInParallel((start, end) -> {
            List<Map<String, List<Integer>>> threadSubset = new ArrayList<>();
            Map<String , List<Integer>> groups = new HashMap<>(); // {group: [indexes]}

            //System.out.println("(" + start + ", " + (start + ((end - start) / 2)) + ", " + end + ")");
            for (int i = start; i < end; i++) {
                if (i == (start + ((end - start) / 2))) {
                    //System.out.println("TRADE");
                    threadSubset.add(groups);
                    groups = new HashMap<>();
                }

                // Finding group of individual
                Float[] tempGroup = new Float[features.length];
                for (int j = 0; j < features.length; j++) {
                    tempGroup[j] = dataset.get(i).get(features[j]);
                }
                String group = Arrays.toString(tempGroup);

                // Data needed to process
                if (!groups.containsKey(group)) {
                    groups.put(group, new ArrayList<>());
                }
                List<Integer> groupData = groups.get(group);
                List<Float> currentIndividual = dataset.get(i);

                // Processing aggregated operation
                List<Float> currentIndividualUpdated = aggregatedWork.doAggregated(groupData, currentIndividual, dataset, 0);

                // Updating the dataset
                dataset.set(i, currentIndividualUpdated);

                // Updating the group information
                groupData.add(i);
                groups.put(group, groupData);
            }
            threadSubset.add(groups);
            return threadSubset;
        }, dataset.size(), nthreads);

        List<Map<String, List<Integer>>> subsets = new ArrayList<>();
        for (List<Map<String, List<Integer>>> threadSubset : preSubsets) {
            while (!threadSubset.isEmpty()) {
                subsets.add(threadSubset.remove(0));
            }
        }

        String g = new ArrayList<>(subsets.get(1).keySet()).get(0);
        System.out.println(g);
        System.out.println("PHASE 0");
        int count = 0;
        for (Map<String, List<Integer>> subset : subsets) {
            if (subset.containsKey(g)) {
                System.out.println(count + " = " +subset.get(g));
            }
            else {
                System.out.println(count + " = []");
            }
            count++;
        }

        DoInParallelFrameWork.doInParallel((start, end) -> {
            int subsetIndex1 = start * 2;
            int subsetIndex2 = subsetIndex1 + 1;
            if (subsetIndex2 < subsets.size()) {
                Map<String, List<Integer>> subset1 = subsets.get(subsetIndex1);
                Map<String, List<Integer>> subset2 = subsets.get(subsetIndex2);
                List<String> groups = new ArrayList<>(subset2.keySet());
                for (String group : groups) {
                    if (subset1.containsKey(group)) {
                        List<Integer> previousGroupMembers = new ArrayList<>();
                        previousGroupMembers.addAll(subset1.get(group));
                        List<Integer> groupMembers = subset2.get(group);
                        for (int member : groupMembers) {
                            List<Float> currentIndividual = dataset.get(member);

                            List<Float> currentIndividualUpdated = aggregatedWork.doAggregated(previousGroupMembers, currentIndividual, dataset, 1);

                            dataset.set(member, currentIndividualUpdated);

                            previousGroupMembers.add(member);
                        }
                        subset2.put(group, previousGroupMembers);
                    }
                }
                subsets.set(subsetIndex2, subset2);
            }
            return null;
        }, subsets.size() / 2);

        System.out.println("\nPHASE 1");
        count = 0;
        for (Map<String, List<Integer>> subset : subsets) {
            if (subset.containsKey(g)) {
                System.out.println(count + " = " +subset.get(g));
            }
            else {
                System.out.println(count + " = []");
            }
            count++;
        }

        List<Integer[]> subsetToProcess = new ArrayList<>();
        for (int i = 1; i < subsets.size(); i +=4) {
            if (i + 1 < subsets.size() && i + 2 < subsets.size()) {
                subsetToProcess.add(new Integer[] {i, i + 1});
                subsetToProcess.add(new Integer[] {i, i + 2});
            }
        }

        for (Integer[] i : subsetToProcess) {
            System.out.println(Arrays.toString(i));
        }

        DoInParallelFrameWork.doInParallel((start, end) -> {
            Integer[] subsetIndexes = subsetToProcess.get(start);
            Map<String, List<Integer>> subset1 = subsets.get(subsetIndexes[0]);
            Map<String, List<Integer>> subset2 = subsets.get(subsetIndexes[1]);
            List<String> groups = new ArrayList<>(subset2.keySet());
            for (String group : groups) {
                if (subset1.containsKey(group)) {
                    List<Integer> previousGroupMembers = new ArrayList<>();
                    previousGroupMembers.addAll(subset1.get(group));
                    List<Integer> groupMembers = subset2.get(group);
                    for (int member : groupMembers) {
                        List<Float> currentIndividual = dataset.get(member);

                        List<Float> currentIndividualUpdated = aggregatedWork.doAggregated(previousGroupMembers, currentIndividual, dataset, 2);

                        dataset.set(member, currentIndividualUpdated);

                        previousGroupMembers.add(member);
                    }
                    subset2.put(group, previousGroupMembers);
                }
            }
            subsets.set(subsetIndexes[1], subset2);
            return null;
        }, subsetToProcess.size());

        System.out.println("\nPHASE 2");
        count = 0;
        for (Map<String, List<Integer>> subset : subsets) {
            if (subset.containsKey(g)) {
                System.out.println(count + " = " +subset.get(g));
            }
            else {
                System.out.println(count + " = []");
            }
            count++;
        }
        return subsets;
    }*/

    public static List<List<Float>> doAggregated(List<List<Float>> dataset, int[] features, AggregatedWork aggregatedWork) {
        int defaultNThreads = Runtime.getRuntime().availableProcessors();
        return doAggregated(dataset, features, aggregatedWork, defaultNThreads);
    }

    public static List<List<Float>> doAggregated(List<List<Float>> dataset, int[] features, AggregatedWork aggregatedWork, int nthreads) {
        List<Map<String, List<Integer>>> preSubsets = DoInParallelFrameWork.doInParallel((start, end) -> {
            Map<String , List<Integer>> groups = new HashMap<>();
            for (int i = start; i < end; i++) {
                // Finding group of individual
                Float[] tempGroup = new Float[features.length];
                for (int j = 0; j < features.length; j++) {
                    tempGroup[j] = dataset.get(i).get(features[j]);
                }
                String group = Arrays.toString(tempGroup);
                // Data needed to process
                if (!groups.containsKey(group)) {
                    groups.put(group, new ArrayList<>());
                }
                List<Integer> groupData = groups.get(group);
                // Updating the group information
                groupData.add(i);
                groups.put(group, groupData);
            }
            return groups;
        }, dataset.size(), nthreads);

        Set<String> keys = new HashSet<>();
        for (Map<String, List<Integer>> subset : preSubsets)  {
            keys.addAll(subset.keySet());
        }

        List<String> groups = new ArrayList<>(keys);

        DoInParallelFrameWork.doInParallel((start, end) -> {
            for (int i = start; i < end; i++) {
                List<Integer> groupData = new ArrayList<>();
                String group = groups.get(i);
                for (Map<String, List<Integer>> subset : preSubsets)  {
                    if (subset.containsKey(group)) {
                        groupData.addAll(subset.get(group));
                    }
                }
                for (int j = 0; j < groupData.size(); j++) {
                    List<Float> individual = aggregatedWork.doAggregated(groupData.subList(0, j), dataset.get(groupData.get(j)), dataset);
                    dataset.set(groupData.get(j), individual);
                }
            }
            return null;
        }, keys.size(), nthreads);

        return dataset;
    }
}
