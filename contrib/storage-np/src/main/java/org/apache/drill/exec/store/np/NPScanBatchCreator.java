package org.apache.drill.exec.store.np;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.np.scan.NPSubScan;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.List;

public class NPScanBatchCreator implements BatchCreator<NPSubScan> {
    @Override
    public CloseableRecordBatch getBatch(ExecutorFragmentContext context, NPSubScan config, List<RecordBatch> children) throws ExecutionSetupException {
        System.out.println("NPScanBatchCreator:getBatch invoked...");

        throw new NotImplementedException();
    }
}
