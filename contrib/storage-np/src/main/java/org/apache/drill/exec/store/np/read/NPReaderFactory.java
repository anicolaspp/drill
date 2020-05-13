package org.apache.drill.exec.store.np.read;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedScanFramework;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.store.np.scan.NPSubScan;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Responsible for creating multiple batch readers
 */
public class NPReaderFactory implements ManagedScanFramework.ReaderFactory {

    private final Iterator<NPBatchReader> readers;

    public NPReaderFactory(NPSubScan subScan) {
        this.readers = getReadersFrom(subScan);
    }

    /**
     * Creates an Iterator of Readers where each of them reads a tablet that is located at this node.
     *
     * @param subScan Presents the associated details needed to created multiple readers.
     * @return Collections or Readers, each reads a different tablet in parallel.
     */
    private Iterator<NPBatchReader> getReadersFrom(NPSubScan subScan) {
        if (subScan.getTablets() == null) {
            return Stream.<NPBatchReader>empty().iterator();
        }

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

        System.out.println("reader.getProperties().getSubScanId() = " + reader.getProperties().getSubScanId());

        return reader;
    }
}

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
