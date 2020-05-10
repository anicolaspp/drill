package org.apache.drill.exec.store.np.scan;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.store.np.NPScanSpec;
import org.apache.drill.exec.store.np.NPStoragePlugin;
import org.apache.drill.exec.util.Utilities;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@JsonTypeName("np-scan")
public class NPGroupScan extends AbstractGroupScan {
    
    private static final Logger logger = LoggerFactory.getLogger(NPGroupScan.class);
    
    private final NPStoragePlugin storagePlugin;
    private final NPScanSpec scanSpec;
    private final List<SchemaPath> columns;
    private final String filters;
    
    public NPGroupScan(NPStoragePlugin storagePlugin,
                       NPScanSpec scanSpec) {
        super("no-user");
        
        this.storagePlugin = storagePlugin;
        this.scanSpec = scanSpec;
        this.columns = ALL_COLUMNS;
        this.filters = "";
    }
    
    public NPGroupScan(NPGroupScan from,
                       List<SchemaPath> columns,
                       String filters) {
        super(from);
        
        this.storagePlugin = from.storagePlugin;
        this.scanSpec = from.scanSpec;
        this.columns = columns;
        this.filters = filters;
    }
    
    @JsonCreator
    public NPGroupScan(@JacksonInject NPStoragePlugin storagePlugin,
                       @JsonProperty("columns") List<SchemaPath> columns,
                       @JsonProperty("scanSpec") NPScanSpec scanSpec,
                       @JsonProperty("filters") String filters) {
        super("no-user");
        
        this.storagePlugin = storagePlugin;
        this.columns = columns;
        this.scanSpec = scanSpec;
        this.filters = filters;
    }
    
    @Override
    public SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException {
        System.out.println(String.format("getSpecificScan with ID: %d invoked...", minorFragmentId));
        
        return new NPSubScan(scanSpec, columns, filters, minorFragmentId);
    }
    
    @Override
    public boolean canPushdownProjects(List<SchemaPath> columns) {
        return true;
    }
    
    @Override
    public int getMaxParallelizationWidth() {
        return 5;
    }
    
    @Override
    public int getMinParallelizationWidth() {
            return 4;
    }
    
    @Override
    public void applyAssignments(List<CoordinationProtos.DrillbitEndpoint> endpoints) throws PhysicalOperatorSetupException {
        System.out.println(String.format("There are %d brillbits available for assigment", endpoints.size()));
        
        endpoints.forEach(bit -> System.out.println(bit.getControlPort()));
    }
    
    @Override
    public String getDigest() {
        return null;
    }
    
    @Override
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
        Preconditions.checkArgument(children.isEmpty());
        
        return new NPGroupScan(this, columns, filters);
    }
    
    @Override
    public ScanStats getScanStats() {
        
        // If this config allows filters, then make the default
        // cost very high to force the planner to choose the version
        // with filters.
//        if (allowsFilters() && !hasFilters()) {
//            return new ScanStats(ScanStats.GroupScanProperty.ESTIMATED_TOTAL_COST,
//                    1E9, 1E112, 1E12);
//        }
        
        // No good estimates at all, just make up something.
        double estRowCount = 10_000;
        
        // NOTE this was important! if the predicates don't make the query more
        // efficient they won't get pushed down
//        if (hasFilters()) {
//            estRowCount *= filterSelectivity;
//        }
        
        double estColCount = Utilities.isStarQuery(columns) ? DrillScanRel.STAR_COLUMN_COST : columns.size();
        double valueCount = estRowCount * estColCount;
        double cpuCost = valueCount;
        double ioCost = valueCount;
        
        // Force the caller to use our costs rather than the
        // defaults (which sets IO cost to zero).
        return new ScanStats(ScanStats.GroupScanProperty.NO_EXACT_ROW_COUNT,
                estRowCount, cpuCost, ioCost);
    }
    
    @Override
    public GroupScan clone(List<SchemaPath> columns) {
        return new NPGroupScan(this, columns, filters);
    }
    
    @Override
    public String toString() {
        return new PlanStringBuilder(this)
                .field("scanSpec", scanSpec)
                .field("columns", columns)
                .field("storagePlugin", storagePlugin)
                .field("filters", filters)
                .toString();
    }
    
    @JsonProperty("columns")
    public List<SchemaPath> columns() {
        return columns;
    }
    
    @JsonProperty("scanSpec")
    public NPScanSpec getScanSpec() {
        return scanSpec;
    }
    
    @JsonIgnore
    public NPStoragePlugin getStoragePlugin() {
        return storagePlugin;
    }
    
    @JsonProperty("filters")
    public String getFilters() {
        return filters;
    }
}
