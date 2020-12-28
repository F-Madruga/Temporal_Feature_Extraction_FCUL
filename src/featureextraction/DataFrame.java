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
                "data=" + data +
                ", columnsNames=" + columnsNames +
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

    public DataFrame filter(/*lambda*/) {
        // TODO: filtra os dados (criar interface para filtrar, desta forma podemos usar lambdas)
        return null;
    }

    public DataFrame sum() {
        Set<String> keys = this.columnsNames.keySet();
        return this.sum(keys.toArray(new String[keys.size()]));
    }

    public DataFrame sum(String[] columns /*WHERE*/) {
        // TODO: Retornar a soma por colunas especificadas
        int[] indexes = new int[columns.length];
        for (int i = 0; i < columns.length; i++) {
            indexes[i] = columnsNames.get(columns[i]);
        }
        List<int []> results = DoInParallelFrameWork.doInParallel(((start, end) -> {
            int[] result = new int[indexes.length];
            for (int i = start; i < end; i++) {
                for (int j = 0; j < indexes.length; j++) {
                    result[j] += (Integer) data.get(i).get(indexes[j]);
                }
            }
            return result;
        }), data.size());
        List<Integer> sum = new ArrayList<>();
        for (int j = 0; j < indexes.length; j++) {
            sum.add(0);
        }
        for (int i = 0; i < results.size(); i++) {
            for (int j = 0; j < results.get(i).length; j++) {
                sum.set(j, sum.get(j) + results.get(i)[j]);
            }
        }
        List<List<?>> total = new ArrayList<>();
        total.add(sum);
        DataFrame df = new DataFrame(total);
        df.setColumnsNames(columns);
        return df;
    }

    public DataFrame mean() {
        Set<String> keys = this.columnsNames.keySet();
        return this.mean(keys.toArray(new String[keys.size()]));
    }

    public DataFrame mean(String[] columns /*Lambda com WHERE*/) {
        // TODO: Retornar a média por colunas especificadas (utiliza o sum)
        return null;
    }
}
