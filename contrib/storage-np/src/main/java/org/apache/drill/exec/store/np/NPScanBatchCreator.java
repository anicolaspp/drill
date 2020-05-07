package org.apache.drill.exec.store.np;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedScanFramework;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.np.scan.NPSubScan;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.ojai.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class NPScanBatchCreator implements BatchCreator<NPSubScan> {

    @Override
    public CloseableRecordBatch getBatch(ExecutorFragmentContext context, NPSubScan subScan, List<RecordBatch> children) throws ExecutionSetupException {
        Preconditions.checkArgument(children.isEmpty());


        return createBuilder(context.getOptions(), subScan)
                .buildScanOperator(context, subScan);
    }

    private ManagedScanFramework.ScanFrameworkBuilder createBuilder(OptionManager optionValues,
                                                                    NPSubScan subScan) {

        ManagedScanFramework.ScanFrameworkBuilder builder = new ManagedScanFramework.ScanFrameworkBuilder();
        builder.projection(subScan.getColumns());
        builder.setUserName(subScan.getUserName());

        ManagedScanFramework.ReaderFactory readerFactory = new NPReaderFactory(subScan);
        builder.setReaderFactory(readerFactory);
        builder.nullType(Types.optional(TypeProtos.MinorType.VARCHAR));

        return builder;
    }

    private static class NPReaderFactory implements ManagedScanFramework.ReaderFactory {

        private NPSubScan subScan;

        public NPReaderFactory(NPSubScan subScan) {
            this.subScan = subScan;
        }

        @Override
        public void bind(ManagedScanFramework framework) {

        }

        @Override
        public ManagedReader<? extends SchemaNegotiator> next() {
            return new ManagedReader<SchemaNegotiator>() {
                private ResultSetLoader loader;

                private final Random random = new Random();

                VectorContainerWriter writer;

                int count = 0;

                @Override
                public boolean open(SchemaNegotiator negotiator) {

                    loader = negotiator.build();

                    writer = new VectorContainerWriter()

                    return true;
                }

                @Override
                public boolean next() {

                    List<Document> documents = new ArrayList<>();


                    while (!rowWriter.isFull()) {
                        rowWriter.start();
                        rowWriter.addRow(random.nextInt(), random.nextInt(), random.nextLong());
                        rowWriter.save();
                    }

                    return count <= 10;
                }

                @Override
                public void close() {

                }
            };
        }
    }
}
