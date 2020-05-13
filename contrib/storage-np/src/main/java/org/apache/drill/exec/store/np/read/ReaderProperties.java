package org.apache.drill.exec.store.np.read;

import org.apache.drill.common.expression.SchemaPath;

import java.util.List;

/**
 * Properties pass to the reader.
 */
class ReaderProperties {
    private final String connectionString;
    private final List<SchemaPath> projections;
    private final String ojaiJsonFilters;
    private final String tableName;
    private final Integer subScanId;

    public ReaderProperties(String connectionString,
                            List<SchemaPath> projections,
                            String ojaiJsonFilters,
                            String tableName, Integer subScanId) {

        this.connectionString = connectionString;
        this.projections = projections;
        this.ojaiJsonFilters = ojaiJsonFilters;
        this.tableName = tableName;
        this.subScanId = subScanId;
    }

    public String getConnectionString() {
        return connectionString;
    }

    public List<SchemaPath> getProjections() {
        return projections;
    }

    public String getOjaiJsonFilters() {
        return ojaiJsonFilters;
    }

    public String getTableName() {
        return tableName;
    }

    public Integer getSubScanId() {
        return subScanId;
    }
}
