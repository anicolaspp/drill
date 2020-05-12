package org.apache.drill.exec.store.np.read;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedScanFramework;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.store.np.scan.NPSubScan;

import java.util.Iterator;
import java.util.List;

/**
 * Responsible for creating multiple batch readers
 */
public class NPReaderFactory implements ManagedScanFramework.ReaderFactory {

    private final Iterator<NPBatchReader> readers;

    public NPReaderFactory(NPSubScan subScan) {
        this.readers = getReadersFrom(subScan);
    }

    private Iterator<NPBatchReader> getReadersFrom(NPSubScan subScan) {
        return subScan
                .getTablets()
                .stream()
                .map(tablet -> {
                    ReaderProperties props = new ReaderProperties(
                            subScan.getScanSpec().getPluginConfig().getConnection(),
                            subScan.getColumns(),
                            subScan.getFilters(), // add tablet filters here
                            subScan.getScanSpec().getTableName(),
                            subScan.getSubScanId()
                    );

                    return new NPBatchReader(props);
                })
                .iterator();
    }

    @Override
    public void bind(ManagedScanFramework framework) {
    }

    @Override
    public ManagedReader<? extends SchemaNegotiator> next() {
        if (!this.readers.hasNext()) {
            return null;
        }

        NPBatchReader reader = readers.next();

        System.out.println("NPReaderFactory:next(); subScanId = " + reader.getProperties().getSubScanId());

        return reader;
    }
}

class ReaderProperties {
    private final String connectionString;
    private final List<SchemaPath> projections;
    private final String ojaiJsonFilters;
    private String tableName;
    private Integer subScanId;

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
