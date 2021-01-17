package featureextraction;

import java.util.List;

@FunctionalInterface
public interface AggregatedWork {
    List<Float> doAggregated(List<Integer> groupIndexes, List<Float> currentIndividual, List<List<Float>> dataset);
}
