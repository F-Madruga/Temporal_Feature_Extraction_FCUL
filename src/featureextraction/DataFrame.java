package featureextraction;

import doinparallel.DoInParallelFrameWork;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class DataFrame {

    private List<List<?>> data;

    private Map<String, Integer> columnsNames;

    public static DataFrame readCSV(String path, String separator, boolean columnNameExist) {
        // TODO: Read csv in parallel and create DataFrame Object
        List<List<?>> data = new ArrayList<>();
        Map<String, Integer> columnsNames = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] rowData = line.split(separator);
                if (columnNameExist) {
                    for (int i = 0; i < rowData.length; i++) {
                        columnsNames.put(rowData[i], i);
                    }
                    columnNameExist = false;
                }
                else {
                    List<Object> row = new ArrayList<>();
                    for (String cell : rowData) {
                        if (cell.matches("^[-+]?\\d*$")) {
                            row.add(new Integer(cell));
                        }
                        else if (cell.matches("((\\+|-)?([0-9]+)(\\.[0-9]+)?)|((\\+|-)?\\.?[0-9]+)")) {
                            row.add(new Float(cell));
                        }
                        else {
                            row.add(cell);
                        }
                    }
                    data.add(row);
                }
            }
        } catch (IOException e) {
            System.out.println("Error reading CSV file");
            e.printStackTrace();
        }
        return new DataFrame(data, columnsNames);
    }

    public DataFrame(List<List<?>> data) {
        this.data = data;
        /* Map<String, Integer> columnsNames = new HashMap<>();
        for (int i = 0; i < this.data.size(); i++) {
            columnsNames.put("Column_" + i, i);
        } */
    }

    public DataFrame(List<List<?>> data, Map<String, Integer> columnsNames) {
        this(data);
        this.columnsNames = columnsNames;
    }

    public DataFrame(List<List<?>> data, String[] columns) {
        this(data);
        this.columnsNames = new HashMap<>();
        for (int i = 0; i < columns.length; i++) {
            columnsNames.put(columns[i], i);
        }
    }

    /*public DataFrame(Map<String, List<Object>> data) {
        this(new ArrayList<>());
        this.columnsNames = new HashMap<>();
        int index = 0;
        for (String column : data.keySet()) {
            this.data.add(data.get(column));
            this.columnsNames.put(column, index);
            index++;
        }
    }*/

    public List<List<?>> getData() {
        return this.data;
    }

    public Map<String, Integer> getColumnsNames() {
        return this.columnsNames;
    }

    public void setColumnsNames(String[] columns) {
        this.columnsNames = new HashMap<>();
        for (int i = 0; i < columns.length; i++) {
            columnsNames.put(columns[i], i);
        }
    }

    @Override
    public String toString() {
        return "DataFrame{" +
                "data=" + this.data +
                ", columnsNames=" + this.columnsNames +
                '}';
    }

    public DataFrame slice(int start, int end) {
        // TODO: Retorna as linhas desde o start (inclusive) até ao end (exclusive)
        return null;
    }

    public DataFrame getColumn(String[] columns) {
        // TODO: Retorna um DataFrame com os dados das colunas pedidas
        return null;
    }

    public DataFrame min() {
        Set<String> keys = this.columnsNames.keySet();
        return this.min(keys.toArray(new String[keys.size()]));
    }

    public DataFrame min(String[] columns) {
        List<Object []> results = DoInParallelFrameWork.doInParallel((start, end) -> {
            Object[] result = new Object[columns.length];
            for (int i = start; i < end; i++) {
                for (int j = 0; j < columns.length; j++) {
                    int columnIndex = this.columnsNames.get(columns[j]);
                    Object cell = data.get(i).get(columnIndex);
                    if (result[j] == null) {
                        result[j] = this.data.get(i).get(columnIndex);
                    }
                    else {
                        if (cell instanceof Integer) {
                            Integer cellValue = (Integer) cell;
                            if (cellValue < (Integer) result[j]) {
                                result[j] = cellValue;
                            }
                        }
                        else if (cell instanceof Double) {
                            Double cellValue = (Double) cell;
                            if (cellValue < (Double) result[j]) {
                                result[j] = cellValue;
                            }
                        }
                        else if (cell instanceof Float) {
                            Float cellValue = (Float) cell;
                            if (cellValue < (Float) result[j]) {
                                result[j] = cellValue;
                            }
                        }
                        else {
                            // TODO: throw error -> not a number
                        }
                    }
                }
            }
            return result;
        }, this.data.size());
        Object[] min = results.remove(0);
        for (int i = 0; i < results.size(); i++) {
            for (int j = 0; j < results.get(i).length; j++) {
                Object cell = results.get(i)[j];
                if (cell instanceof Integer) {
                    Integer cellValue = (Integer) cell;
                    if (cellValue < (Integer) min[j]) {
                        min[j] = cellValue;
                    }
                }
                else if (cell instanceof Double) {
                    Double cellValue = (Double) cell;
                    if (cellValue < (Double) min[j]) {
                        min[j] = cellValue;
                    }
                }
                else if (cell instanceof Float) {
                    Float cellValue = (Float) cell;
                    if (cellValue < (Float) min[j]) {
                        min[j] = cellValue;
                    }
                }
                else {
                    // TODO: throw error -> not a number
                }
            }
        }
        DataFrame df = new DataFrame(Arrays.asList(Arrays.asList(min)));
        df.setColumnsNames(columns);
        return df;
    }

    public DataFrame max() {
        Set<String> keys = this.columnsNames.keySet();
        return this.max(keys.toArray(new String[keys.size()]));
    }

    public DataFrame max(String[] columns) {
        List<Object []> results = DoInParallelFrameWork.doInParallel((start, end) -> {
            Object[] result = new Object[columns.length];
            for (int i = start; i < end; i++) {
                for (int j = 0; j < columns.length; j++) {
                    int columnIndex = this.columnsNames.get(columns[j]);
                    Object cell = data.get(i).get(columnIndex);
                    if (result[j] == null) {
                        result[j] = this.data.get(i).get(columnIndex);
                    }
                    else {
                        if (cell instanceof Integer) {
                            Integer cellValue = (Integer) cell;
                            if (cellValue > (Integer) result[j]) {
                                result[j] = cellValue;
                            }
                        }
                        else if (cell instanceof Double) {
                            Double cellValue = (Double) cell;
                            if (cellValue > (Double) result[j]) {
                                result[j] = cellValue;
                            }
                        }
                        else if (cell instanceof Float) {
                            Float cellValue = (Float) cell;
                            if (cellValue > (Float) result[j]) {
                                result[j] = cellValue;
                            }
                        }
                        else {
                            // TODO: throw error -> not a number
                        }
                    }
                }
            }
            return result;
        }, this.data.size());
        Object[] max = results.remove(0);
        for (int i = 0; i < results.size(); i++) {
            for (int j = 0; j < results.get(i).length; j++) {
                Object cell = results.get(i)[j];
                if (cell instanceof Integer) {
                    Integer cellValue = (Integer) cell;
                    if (cellValue > (Integer) max[j]) {
                        max[j] = cellValue;
                    }
                }
                else if (cell instanceof Double) {
                    Double cellValue = (Double) cell;
                    if (cellValue > (Double) max[j]) {
                        max[j] = cellValue;
                    }
                }
                else if (cell instanceof Float) {
                    Float cellValue = (Float) cell;
                    if (cellValue > (Float) max[j]) {
                        max[j] = cellValue;
                    }
                }
                else {
                    // TODO: throw error -> not a number
                }
            }
        }
        DataFrame df = new DataFrame(Arrays.asList(Arrays.asList(max)));
        df.setColumnsNames(columns);
        return df;
    }

    public DataFrame filter(ColumnFilter filter) {
        // TODO: filtra os dados (criar interface para filtrar, desta forma podemos usar lambdas)
        List<List<List<?>>> results = DoInParallelFrameWork.doInParallel(((start, end) -> {
            List<List<?>> result = new ArrayList<>();
            for (int i = start; i < end; i++) {
                if (filter.filterBy(this.columnsNames, this.data.get(i))) {
                    result.add(this.data.get(i));
                }
            }
            return result;
        }), data.size());
        int iteration = results.size();
        for (int i = 1; i < iteration; i++) {
            results.get(0).addAll(results.remove(1));
        }
        // DataFrame df = new DataFrame(results.get(0), this.columnsNames);
        return new DataFrame(results.get(0), this.columnsNames);
    }

    public DataFrame sum() {
        Set<String> keys = this.columnsNames.keySet();
        return this.sum(keys.toArray(new String[keys.size()]));
    }

    public DataFrame sum(String[] columns /*WHERE*/) {
        // TODO: Retornar a soma por colunas especificadas
        List<Double []> results = DoInParallelFrameWork.doInParallel(((start, end) -> {
            Double[] result = new Double[columns.length];
            for (int i = start; i < end; i++) {
                for (int j = 0; j < columns.length; j++) {
                    if (result[j] == null) {
                        result[j] = 0.0;
                    }
                    int columnIndex = this.columnsNames.get(columns[j]);
                    Object cell = data.get(i).get(columnIndex);
                    if (cell instanceof Integer) {
                        result[j] += (Integer) data.get(i).get(columnIndex);
                    }
                    else if (cell instanceof Double) {
                        result[j] += (Double) data.get(i).get(columnIndex);
                    }
                    else if (cell instanceof Float) {
                        result[j] += (Integer) data.get(i).get(columnIndex);
                    }
                    else {
                        // TODO: throw error -> not a number
                    }
                }
            }
            return result;
        }), data.size());
        Double [] sum = new Double[columns.length];
        for (Double [] result : results) {
            for (int i = 0; i < result.length; i++) {
                if (sum[i] == null) {
                    sum[i] = result[i];
                }
                else {
                    sum[i] += result[i];
                }
            }
        }
        DataFrame df = new DataFrame(Arrays.asList(Arrays.asList(sum)));
        df.setColumnsNames(columns);
        return df;
    }

    public DataFrame mean() {
        Set<String> keys = this.columnsNames.keySet();
        return this.mean(keys.toArray(new String[keys.size()]));
    }

    public DataFrame mean(String[] columns /*Lambda com WHERE*/) {
        List<List<?>> sum = this.sum(columns).getData();
        List<Double> mean = new ArrayList<>();
        for (Object cell : sum.get(0)) {
            mean.add((Double) cell / this.data.size());
        }
        DataFrame df = new DataFrame(Arrays.asList(mean));
        df.setColumnsNames(columns);
        return df;
    }
}
