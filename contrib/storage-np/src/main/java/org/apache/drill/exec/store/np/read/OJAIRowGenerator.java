package org.apache.drill.exec.store.np.read;

import org.apache.drill.exec.store.np.filter.Filter;
import org.apache.drill.exec.store.np.ojai.ConnectionProvider;
import org.apache.drill.exec.store.np.scan.NPSubScan;
import org.ojai.Document;
import org.ojai.store.Connection;
import org.ojai.store.DocumentStore;
import org.ojai.store.Query;
import org.ojai.store.QueryCondition;

import java.util.Iterator;
import java.util.List;

/**
 * In charge of querying the OJAI source and returning Documents.
 * <p>
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
        
        QueryCondition condition = queryWithFilters(subScan.getFilters());
    
        Query query = connection.newQuery().where(condition).build();
        
        return store.find(query).iterator();
    }
    
    @Override
    public byte[] getBytesFrom(Document value) {
        return value.asJsonString().getBytes();
    }
    
    /**
     * Builds a QueryCondition based on the agreed filters that will be push down to OJAI Context.
     *
     * @param filters filters to be applied.
     * @return QueryCondition to be pushed down. s
     */
    private QueryCondition queryWithFilters(List<Filter> filters) {
        
        QueryCondition andQuery = connection.newCondition().and();
        
        filters.forEach(filter -> andQuery.condition(
                connection.newCondition()
                        .is(filter.getColumn(), QueryCondition.Op.EQUAL, filter.getValue().toString()).build()
        ));
        
        return andQuery.close().build();
    }
}
