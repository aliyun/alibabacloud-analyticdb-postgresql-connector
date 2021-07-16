import java.io.Serializable;
import java.util.ArrayList;

public class AdbpgTableSchema implements Serializable {
    private static final long serialVersionUID = 10L;
    private ArrayList<String> primaryKeys;
    private ArrayList<String> fieldNames;
    private ArrayList<Class<?>> dataTypes;
    private int length;
    public AdbpgTableSchema(ArrayList<String> primaryKeys, ArrayList<String> fieldNames, ArrayList<Class<?>> dataTypes){
        this.primaryKeys = primaryKeys;
        this.fieldNames = fieldNames;
        this.dataTypes = dataTypes;
        length = fieldNames.size();
    }
    public ArrayList<String> getPrimaryKeys() {
        return primaryKeys;
    }
    public int getLength(){
        return length;
    }
    public ArrayList<String> getFieldNames() {
        return fieldNames;
    }
    public ArrayList<Class<?>> getTypes() {
        return dataTypes;
    }
    public int getFieldIndex(String key) {
        for(int i = 0; i < fieldNames.size(); i++) {
            if (key.equalsIgnoreCase(fieldNames.get(i))) {
                return i;
            }
        }
        return -1;
    }
}