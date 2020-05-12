package org.apache.drill.exec.store.np.read;

import org.apache.drill.exec.store.np.ojai.ConnectionProvider;
import org.ojai.Document;
import org.ojai.store.Connection;
import org.ojai.store.DocumentStore;
import org.ojai.store.Query;

import java.util.Iterator;

/**
 * In charge of querying the OJAI source and returning Documents.
 * <p>
 * We might want to have one of this per each Table Region (tablet).
 */
public class OJAIRowGenerator implements RowGenerator<Document>, ConnectionProvider {

    private final Connection connection;
//    private final NPSubScan subScan;
    private ReaderProperties properties;

//    public OJAIRowGenerator(NPSubScan subScan) {
//        this.subScan = subScan;
//        this.connection = connectTo(subScan.getScanSpec().getPluginConfig().getConnection());
//    }

    public OJAIRowGenerator(ReaderProperties properties) {
        this.properties = properties;
        this.connection = connectTo(properties.getConnectionString());
    }

    @Override
    public Iterator<Document> getRows() {
        DocumentStore store = connection.getStore(properties.getTableName());

        String jsonQuery = connection.newDocument(properties.getOjaiJsonFilters()).asJsonString();

        String[] projections = properties
                .getProjections()
                .stream()
                .filter(col -> !col.toExpr().equals("`**`"))
                .map(col -> col.getAsNamePart().getName())
                .toArray(String[]::new);

        Query query = connection
                .newQuery()
                .select(projections)
                .where(jsonQuery).build();

        return store.find(query).iterator();
    }

    @Override
    public byte[] getBytesFrom(Document value) {
        return value.asJsonString().getBytes();
    }
}
