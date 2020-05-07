package org.apache.drill.exec.store.np;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.PlanStringBuilder;

@JsonTypeName("np-scan-spec")
public class NPScanSpec {
    private final String tableName;

    @JsonCreator
    public NPScanSpec(@JsonProperty("tableName") String tableName) {
        System.out.println("Creating NP SCAN SPEC");
        this.tableName = tableName;
    }


    @JsonProperty("tableName")
    public String getTableName() {
        return tableName;
    }


    @Override
    public String toString() {
        return new PlanStringBuilder(this)
                .field("table", tableName)
                .toString();
    }
}
