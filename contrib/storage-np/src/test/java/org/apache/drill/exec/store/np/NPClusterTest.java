package org.apache.drill.exec.store.np;

import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.test.BaseDirTestWatcher;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryBuilder;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class NPClusterTest extends ClusterTest {

    @ClassRule
    public static final BaseDirTestWatcher dirTestWatcher = new BaseDirTestWatcher();

    @BeforeClass
    public static void setup() throws Exception {
        ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));

        defineNPPlugin();
    }

    @SuppressWarnings("source")
    private static void defineNPPlugin() throws StoragePluginRegistry.PluginException {
        Drillbit drillbit = cluster.drillbit();

        NPStoragePluginConfig config = new NPStoragePluginConfig("ojai:mapr:");
        config.setEnabled(true);

        StoragePluginRegistry pluginRegistry = drillbit.getContext().getStorage();
        pluginRegistry.put(NPStoragePluginConfig.NAME, config);
    }

    @Test
    public void testWildCard() throws Exception {
        String sql = "SELECT a from np.my_table";

        QueryBuilder builder = client.queryBuilder().sql(sql);
    
        builder.printCsv();
    }
}
