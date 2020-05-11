package org.apache.drill.exec.store.np.schema;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.exec.store.AbstractSchemaFactory;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.np.NPStoragePlugin;
import org.apache.drill.exec.store.np.ojai.ConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * User by the Plugin to dynamically register the schema our plugin uses.
 */
public class NPSchemaFactory extends AbstractSchemaFactory implements ConnectionProvider {
    private static final Logger logger = LoggerFactory.getLogger(NPSchemaFactory.class);
    
    private final NPStoragePlugin plugin;
    
    public NPSchemaFactory(NPStoragePlugin plugin) {
        super(plugin.getName());
        
        this.plugin = plugin;
    }
    
    @Override
    public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
        NPDefaultSchema npSchema = new NPDefaultSchema(this, plugin, getName());
        
        parent.add(getName(), npSchema);
    }
}