package org.apache.drill.exec.store.np;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.np.scan.NPGroupScan;
import org.apache.drill.exec.store.np.schema.NPSchemaFactory;
import org.ojai.store.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Entry point for NP Storage Plugin
 */
public class NPStoragePlugin extends AbstractStoragePlugin {

    static final Logger logger = LoggerFactory.getLogger(NPStoragePlugin.class);

    /**
     * Plugin configuration
     */
    private final NPStoragePluginConfig config;

    /**
     * OJAI connection to access MapR-DB
     */
    private  Connection ojaiConnection;

    private final NPSchemaFactory schemaFactory;

    public NPStoragePlugin(NPStoragePluginConfig config,
                           DrillbitContext context,
                           String name) {
        super(context, name);

        this.config = config;
        logger.debug("Starting NPPlugin with connectionString=" + config.getConnection());

//        this.ojaiConnection = DriverManager.getConnection(config.getConnection());
        this.schemaFactory = new NPSchemaFactory(this);
    }

    /**
     * Method returns a Jackson serializable object that extends a StoragePluginConfig.
     *
     * @return an extension of StoragePluginConfig
     */
    @Override
    public StoragePluginConfig getConfig() {
        return config;
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
        schemaFactory.registerSchemas(schemaConfig, parent);
    }

    /**
     * Indicates if the Storage Plugin support reading.
     *
     */
    @Override
    public boolean supportsRead() {
        return true;
    }

    @Override
    public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection) throws IOException {
        NPScanSpec scanSpec = selection.getListWith(context.getLpPersistence().getMapper(), new TypeReference<NPScanSpec>() {});

        AbstractGroupScan groupScan = new NPGroupScan(this, scanSpec);

        return groupScan;
    }
}
