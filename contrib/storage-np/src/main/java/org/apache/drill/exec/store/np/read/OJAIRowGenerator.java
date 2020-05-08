package org.apache.drill.exec.store.np.read;

import org.apache.drill.exec.store.np.ojai.ConnectionProvider;
import org.apache.drill.exec.store.np.scan.NPSubScan;
import org.ojai.Document;
import org.ojai.store.Connection;
import org.ojai.store.DocumentStore;

import java.util.Iterator;

/**
 * In charge of querying the OJAI source and returning Documents.
 *
 * We might want to have one of this per each Table Region (tablet).
 */
public class OJAIRowGenerator implements RowGenerator<Document>, ConnectionProvider {

    private final Connection connection;
    private final NPSubScan subScan;

    public OJAIRowGenerator(NPSubScan subScan) {
        this.subScan = subScan;
        this.connection = connectTo(subScan.getScanSpec().getPluginConfig().getConnection());
    }

    @Override
    public Iterator<Document> getRows() {
        DocumentStore store = connection.getStore(subScan.getScanSpec().getTableName());

        // This is the most generic query we can possible do.
        // We will refine this base on filter and projections sometime later.
        return store.find().iterator();
    }

    @Override
    public byte[] getBytesFrom(Document value) {
        return value.asJsonString().getBytes();
    }
}
