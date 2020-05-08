package org.apache.drill.exec.store.np;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedScanFramework;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.np.read.NPReaderFactory;
import org.apache.drill.exec.store.np.scan.NPSubScan;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

import java.util.List;

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
    
}

