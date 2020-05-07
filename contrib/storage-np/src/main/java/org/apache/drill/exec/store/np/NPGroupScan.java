package org.apache.drill.exec.store.np;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos;

import java.util.List;

public class NPGroupScan extends AbstractGroupScan {
    private NPScanSpec npSpec;
    private final List<SchemaPath> columns;
    
    public NPGroupScan(NPScanSpec spec) {
        super("no-user");
        
        this.npSpec = spec;
        this.columns = ALL_COLUMNS;
    }
    
    @Override
    public void applyAssignments(List<CoordinationProtos.DrillbitEndpoint> endpoints) throws PhysicalOperatorSetupException {
    
    }
    
    @Override
    public SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException {
        return null;
    }
    
    @Override
    public int getMaxParallelizationWidth() {
        return 1;
    }
    
    /**
     * Returns a signature of the {@link GroupScan} which should usually be composed of
     * all its attributes which could describe it uniquely.
     */
    @Override
    public String getDigest() {
        return toString();
    }
    
    /**
     * Regenerate with this node with a new set of children.  This is used in the case of materialization or optimization.
     *
     * @param children
     */
    @Override
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
        return null;
    }
}
