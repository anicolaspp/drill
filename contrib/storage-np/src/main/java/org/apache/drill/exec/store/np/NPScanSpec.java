package org.apache.drill.exec.store.np;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.PlanStringBuilder;

@JsonTypeName("np-scan-spec")
public class NPScanSpec {
    
    private final String pluginName;
    private final String connectionName;
    private final String tableName;
    private final NPStoragePluginConfig config;
    
    @JsonCreator
    public NPScanSpec(@JsonProperty("pluginName") String pluginName,
                      @JsonProperty("connectionName") String connectionName,
                      @JsonProperty("tableName") String tableName,
                      @JsonProperty("config") NPStoragePluginConfig config) {
        
        this.pluginName = pluginName;
        this.connectionName = connectionName;
        this.tableName = tableName;
        this.config = config;
    }
    
    @JsonProperty("pluginName")
    public String getPluginName() {
        return pluginName;
    }
    
    @JsonProperty("connectionName")
    public String getConnectionName() {
        return connectionName;
    }
    
    @JsonProperty("tableName")
    public String getTableName() {
        return tableName;
    }
    
    @JsonProperty("config")
    public NPStoragePluginConfig getConfig() {
        return config;
    }
    
    @Override
    public String toString() {
        return new PlanStringBuilder(this)
                .field("schemaName", pluginName)
                .field("table", tableName)
                .field("config", config)
                .toString();
    }
}
