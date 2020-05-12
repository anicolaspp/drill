package org.apache.drill.exec.store.np.schema;

import org.apache.calcite.schema.Table;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.np.NPScanSpec;
import org.apache.drill.exec.store.np.NPStoragePlugin;
import org.apache.drill.exec.store.np.NPStoragePluginConfig;
import org.ojai.store.Connection;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * In charge of dynamically exploring our storage schema.
 * <p>
 * In our case, we need to make sure we are querying OJAI tables and that
 * those actually exist in our context.
 * <p>
 * The context is any OJAI enabled container. Normally it would be MapR-DB, but
 * OJAI-Testing enables In-Memory (embedded) OJAI which is also a context.
 */
class NPDefaultSchema extends AbstractSchema {

    private final NPSchemaFactory npSchemaFactory;
    private final Map<String, DynamicDrillTable> activeTables = new HashMap<>();

    private final NPStoragePlugin plugin;

    public NPDefaultSchema(NPSchemaFactory npSchemaFactory, NPStoragePlugin plugin, String name) {
        super(Collections.emptyList(), name);
        this.npSchemaFactory = npSchemaFactory;

        this.plugin = plugin;
    }

    @Override
    public Table getTable(String tableName) {
        DynamicDrillTable table = activeTables.get(tableName);

        if (table != null) {
            return table;
        }

        if (!isValidTable(tableName)) {
            return null;
        }

        return registerTable(tableName,
                new DynamicDrillTable(
                        plugin,
                        plugin.getName(),
                        new NPScanSpec(tableName, (NPStoragePluginConfig) plugin.getConfig())
                )
        );
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
     * Verifies that the requested tableName is valid and exist in our context.
     *
     * @param tableName Requested table name
     * @return true is the table name is valid in the context.
     */
    private boolean isValidTable(String tableName) {
        NPStoragePluginConfig config = (NPStoragePluginConfig) plugin.getConfig();

        Connection connection = npSchemaFactory.connectTo(config.getConnection());

        return connection.storeExists(tableName);
    }

    private DynamicDrillTable registerTable(String name, DynamicDrillTable table) {
        activeTables.put(name, table);

        return table;
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
