package org.apache.drill.exec.store.np;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.logical.StoragePluginConfig;

/**
 * NPStoragePluginConfig configuration.
 * <p>
 * This class defines the configuration properties of the Storage Plugin.
 */
@JsonTypeName(NPStoragePluginConfig.NAME)
public class NPStoragePluginConfig extends StoragePluginConfig {
    public static final String NAME = "np";
    
    private final String connection;
    
    @JsonCreator
    public NPStoragePluginConfig(@JsonProperty("connection") String connection) {
        this.connection = connection;
    }
    
    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        } else if (that == null || getClass() != that.getClass()) {
            return false;
        }
        NPStoragePluginConfig thatConfig = (NPStoragePluginConfig) that;
        
        return this.connection.equals(thatConfig.connection);
    }
    
    @Override
    public int hashCode() {
        return this.connection != null ? this.connection.hashCode() : 0;
    }
    
    public String getConnection() {
        return connection;
    }
}
