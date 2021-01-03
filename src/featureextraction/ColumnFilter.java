package featureextraction;

import java.util.List;
import java.util.Map;

public interface ColumnFilter {
    public boolean filterBy(Map<String, Integer> columnsNames, List<?> row);
}
