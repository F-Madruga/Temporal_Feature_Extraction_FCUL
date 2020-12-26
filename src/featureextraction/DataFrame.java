package featureextraction;

import java.util.*;

public class DataFrame {

    private List<List<Object>> data;

    private Map<String, Integer> columnsNames;


    public static DataFrame readCSV(String path, String separator, boolean columnNameExist) {
        // TODO: Read csv in parallel and create DataFrame Object
        return null;
    }

    public DataFrame(List<List<Object>> data, Map<String, Integer> columnsNames) {
        this.data = data;
        this.columnsNames = columnsNames;
    }

    public DataFrame(List<List<Object>> data, String[] columns) {
        this.columnsNames = new HashMap<>();
        for (int i = 0; i < columns.length; i++) {
            columnsNames.put(columns[i], i);
        }
    }

    public DataFrame(List<List<Object>> data) {
        this.columnsNames = new HashMap<>();
        for (int i = 0; i < this.data.size(); i++) {
            this.columnsNames.put("Column_" + i, i);
        }
    }

    public DataFrame(Map<String, List<Object>> data) {
        this.data = new ArrayList<>();
        this.columnsNames = new HashMap<>();
        int index = 0;
        for (String column : data.keySet()) {
            this.data.add(data.get(column));
            this.columnsNames.put(column, index);
            index++;
        }
    }

    public List<List<Object>> getData() {
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

    public DataFrame sum(String[] columns /*Lambda com WHERE*/) {
        // TODO: Retornar a soma por colunas especificadas
        return null;
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
