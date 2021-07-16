import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.StringUtils;

public class ConnectionPool<T> {
    private final Map<String, T> pools = new ConcurrentHashMap();
    private final Map<String, Integer> referenceCounts = new ConcurrentHashMap();
    private static final String SUBKEY_DELIMETER = "@";

    public ConnectionPool() {
    }

    public synchronized boolean contains(String dataSourceName) {
        return this.pools.containsKey(dataSourceName);
    }

    public synchronized T get(String dataSourceName) {
        this.referenceCounts.put(dataSourceName, (Integer)this.referenceCounts.get(dataSourceName) + 1);
        return this.pools.get(dataSourceName);
    }

    public synchronized T put(String dataSourceName, T dataSource) {
        this.referenceCounts.put(dataSourceName, 1);
        return this.pools.put(dataSourceName, dataSource);
    }

    public synchronized boolean remove(String dataSourceName) {
        Integer count = (Integer)this.referenceCounts.get(dataSourceName);
        if (count == null) {
            return false;
        } else if (count == 1) {
            this.referenceCounts.remove(dataSourceName);
            this.pools.remove(dataSourceName);
            return true;
        } else {
            this.referenceCounts.put(dataSourceName, count - 1);
            return false;
        }
    }

    private String genCombinedKey(String... keys) {
        return StringUtils.join("@", keys);
    }

    public synchronized boolean contains(String dataSourceName, String subKeyName) {
        return this.contains(this.genCombinedKey(dataSourceName, subKeyName));
    }

    public synchronized T get(String dataSourceName, String subKeyName) {
        return this.get(this.genCombinedKey(dataSourceName, subKeyName));
    }

    public synchronized T put(String dataSourceName, String subKeyName, T dataSource) {
        return this.put(this.genCombinedKey(dataSourceName, subKeyName), dataSource);
    }

    public synchronized boolean remove(String dataSourceName, String subKeyName) {
        return this.remove(this.genCombinedKey(dataSourceName, subKeyName));
    }

    public synchronized int size() {
        return this.pools.size();
    }
}
