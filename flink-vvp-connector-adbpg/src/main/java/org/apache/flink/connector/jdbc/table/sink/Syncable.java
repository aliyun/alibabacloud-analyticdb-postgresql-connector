package org.apache.flink.connector.jdbc.table.sink;

import java.io.IOException;

public interface Syncable {

    void sync() throws IOException;

    void waitFinish() throws Exception;
}
