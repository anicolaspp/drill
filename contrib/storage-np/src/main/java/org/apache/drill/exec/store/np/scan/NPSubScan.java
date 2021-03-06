package org.apache.drill.exec.store.np.scan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.store.np.NPScanSpec;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

@JsonTypeName("np-sub-scan")
public class NPSubScan extends AbstractBase implements SubScan {

    private final NPScanSpec scanSpec;
    private final List<SchemaPath> columns;
    private final String filters;
    private final Integer subScanId ;
    private ArrayList<NPTabletInfo> tablets;

    @JsonCreator
    public NPSubScan(@JsonProperty("scanSpec") NPScanSpec scanSpec,
                     @JsonProperty("columns") List<SchemaPath> columns,
                     @JsonProperty("filters") String filters,
                     @JsonProperty("subScanId") Integer subScanId,
                     @JsonProperty("tablets") ArrayList<NPTabletInfo> tablets) {
        super("user-if-needed");

        this.scanSpec = scanSpec;
        this.columns = columns;
        this.filters = filters;
        this.subScanId = subScanId;
        this.tablets = tablets;
    }

    @Override
    public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
        return physicalVisitor.visitSubScan(this, value);
    }

    @Override
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
        return new NPSubScan(scanSpec, columns, filters, subScanId, tablets);
    }

    @Override
    public int getOperatorType() {
        return 90;
    }

    @Override
    public Iterator<PhysicalOperator> iterator() {
        return ImmutableSet.<PhysicalOperator>of().iterator();
    }

    @Override
    public String toString() {
        return new PlanStringBuilder(this)
                .field("scanSpec", scanSpec)
                .field("columns", columns)
                .field("filters", filters)
                .field("scanId", subScanId)
                .toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(scanSpec, columns, filters);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        NPSubScan other = (NPSubScan) obj;
        return Objects.equals(scanSpec, other.scanSpec)
                && Objects.equals(columns, other.columns)
                && Objects.equals(filters, other.filters);
    }

    @JsonProperty("scanSpec")
    public NPScanSpec getScanSpec() {
        return scanSpec;
    }

    @JsonProperty("columns")
    public List<SchemaPath> getColumns() {
        return columns;
    }

    @JsonProperty("filters")
    public String getFilters() {
        return filters.isEmpty() ? "{}" : filters;
    }

    @JsonProperty("subScanId")
    public Integer getSubScanId() {
        return this.subScanId;
    }

    @JsonProperty("tablets")
    public ArrayList<NPTabletInfo> getTablets() {
        return this.tablets;
    }
}
