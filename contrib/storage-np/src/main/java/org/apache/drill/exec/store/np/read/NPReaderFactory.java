package org.apache.drill.exec.store.np.read;

import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedScanFramework;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.store.np.scan.NPSubScan;

/**
 * Responsible for creating multiple batch readers
 */
public class NPReaderFactory implements ManagedScanFramework.ReaderFactory {
    
    private NPSubScan subScan;
    
    private NPBatchReader reader;
    
    int numberOfReader = 1;
    
    public NPReaderFactory(NPSubScan subScan) {
        this.subScan = subScan;
        this.reader = new NPBatchReader(subScan);
    }
    
    @Override
    public void bind(ManagedScanFramework framework) {
    }
    
    @Override
    public ManagedReader<? extends SchemaNegotiator> next() {
        if (numberOfReader == 0) {
            return null;
        }
        
        System.out.println("NPReaderFactory:next(); subScanId = " + subScan.getSubScanId());

        numberOfReader--;
        
        return reader;
    }
}
