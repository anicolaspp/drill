package org.apache.drill.exec.store.np.schema;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.AbstractSchemaFactory;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.np.NPScanSpec;
import org.apache.drill.exec.store.np.NPStoragePlugin;
import org.apache.drill.exec.store.np.NPStoragePluginConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class NPSchemaFactory extends AbstractSchemaFactory {
    private static final Logger logger = LoggerFactory.getLogger(NPSchemaFactory.class);

    private final NPStoragePlugin plugin;

    public NPSchemaFactory(NPStoragePlugin plugin) {
        super(plugin.getName());

        this.plugin = plugin;
    }

    @Override
    public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
        NPDefaultSchema npSchema = new NPDefaultSchema(plugin, getName());

        parent.add(getName(), npSchema);
    }

    class NPDefaultSchema extends AbstractSchema {

        private final Map<String, DynamicDrillTable> activeTables = new HashMap<>();

        private final NPStoragePlugin plugin;

        public NPDefaultSchema(NPStoragePlugin plugin, String name) {
            super(Collections.emptyList(), name);

            this.plugin = plugin;
        }

        @Override
        public Table getTable(String name) {
            DynamicDrillTable table = activeTables.get(name);

            if (table != null) {
                return table;
            }

            // TODO: check if the table exists in MFS and register it accordingly

            return registerTable(name,
                    new DynamicDrillTable(
                            plugin,
                            plugin.getName(),
                            new NPScanSpec(name, (NPStoragePluginConfig) plugin.getConfig())
                    )
            );
        }

        private DynamicDrillTable registerTable(String name, DynamicDrillTable table) {
            activeTables.put(name, table);

            return table;
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
