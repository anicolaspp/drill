package org.apache.drill.exec.store.np;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.AbstractSchemaFactory;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.StoragePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;

public class NPSchemaFactory extends AbstractSchemaFactory {
    private static final Logger logger = LoggerFactory.getLogger(NPSchemaFactory.class);
    
    private final NPStoragePlugin plugin;
    
    public NPSchemaFactory(NPStoragePlugin plugin) {
        super(plugin.getName());
        
        this.plugin = plugin;
    }
    
    /**
     * Register the schemas provided by this SchemaFactory implementation under the given parent schema.
     *
     * @param schemaConfig Configuration for schema objects.
     * @param parent       Reference to parent schema.
     * @throws IOException in case of error during schema registration
     */
    @Override
    public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
        NPSchema schema = new NPSchema(plugin);
    
        logger.debug("Registering {} {}", schema.getName(), schema.toString());
        
        SchemaPlus schemaPlus = parent.add(getName(), schema);
        
        schemaPlus.add(getName(), schema);
    }
    
    class NPSchema extends AbstractSchema {
    
        private final NPStoragePlugin plugin;
    
        public NPSchema(NPStoragePlugin plugin) {
            super(Collections.emptyList(), plugin.getName());
            this.plugin = plugin;
        }
    
        /**
         * Returns string describing schema type which shows where the schema came from.
         * Good practice here is to return json type name of storage plugin's config.
         *
         * @return schema type name
         */
        @Override
        public String getTypeName() {
            return NPStoragePluginConfig.NAME;
        }
    
        /**
         * The Storage Plugin associated with this Schema, that is {@code NPStoragePlugin}.
         * 
         * @return Storage Plugin
         */
        public StoragePlugin getPlugin() {
            return plugin;
        }
    }
}
