package org.apache.drill.exec.store.np.read;

import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedScanFramework;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.store.np.scan.NPSubScan;

import java.util.Iterator;
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
                            addTabletQueryRangeTo(subScan.getFilters(), tablet.getFilter()),
                            subScan.getScanSpec().getTableName(),
                            subScan.getSubScanId()
                    );

                    return new NPBatchReader(props);
                })
                .iterator();
    }

    /**
     * Decides how to build the final OJAI query to be executed by the corresponding reader.
     *
     * @param filter           OJAI query built from the query filters (WHERE clause).
     * @param tabletQueryRange OJAI query that identify the tablet being query (OJAI Testing is {} since it is in
     *                         memory and there are not tables.
     * @return Final OJAI query (AND) to added as OJAI where to the query to be executed.
     */
    private String addTabletQueryRangeTo(String filter, String tabletQueryRange) {
        if (!filter.equals("{}") && !tabletQueryRange.equals("{}")) {
            return String.format("{\"$and\":[%s,%s]}", filter, tabletQueryRange);
        }

        if (filter.equals("{}")) {
            return tabletQueryRange;
        }

        if (tabletQueryRange.equals("{}")) {
            return filter;
        }

        return "";
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

