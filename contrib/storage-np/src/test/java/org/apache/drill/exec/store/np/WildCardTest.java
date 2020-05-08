package org.apache.drill.exec.store.np;

import com.github.anicolaspp.ojai.JavaOjaiTesting;
import org.apache.drill.exec.physical.rowSet.DirectRowSet;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.np.ojai.SmartConnectionProvider;
import org.apache.drill.test.BaseDirTestWatcher;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryBuilder;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.ojai.Document;
import org.ojai.store.Connection;
import org.ojai.store.DocumentStore;

/**
 * For testing wild card test against in-memory store
 */
public class WildCardTest extends ClusterTest {
    @ClassRule
    public static final BaseDirTestWatcher dirTestWatcher = new BaseDirTestWatcher();

    private static final JavaOjaiTesting ojai = new JavaOjaiTesting();

    @BeforeClass
    public static void setup() throws Exception {
        ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));

        defineNPPlugin();

        genData();
    }

    @SuppressWarnings("source")
    private static void defineNPPlugin() throws StoragePluginRegistry.PluginException {
        Drillbit drillbit = cluster.drillbit();

        NPStoragePluginConfig config = new NPStoragePluginConfig("ojai:anicolaspp:mem");
        config.setEnabled(true);

        StoragePluginRegistry pluginRegistry = drillbit.getContext().getStorage();
        pluginRegistry.put(NPStoragePluginConfig.NAME, config);
    }

    public static void genData() {

        Connection connection = SmartConnectionProvider
                .getInstance()
                .connectTo("ojai:anicolaspp:mem");

        DocumentStore store = connection.getStore("/user/store1");

        for (int i = 0; i < 100; i++) {
            Document doc = ojai
                    .getConnection()
                    .newDocument()
                    .setId(String.valueOf(i))
                    .set("value", i);

            store.insert(doc);
        }
    }


    @Test
    public void testReadAllRecords() throws Exception {
        String sql = "SELECT * FROM np.`/user/store1`";

        QueryBuilder builder = client.queryBuilder().sql(sql);

        assert builder.run().recordCount() == 100;
    }

    @Test
    public void testProjections() throws RpcException {
        String sql = "SELECT _id FROM np.`/user/store1`";

        QueryBuilder builder = client.queryBuilder().sql(sql);

        DirectRowSet directRowSet = builder.rowSet();

        assert directRowSet.schema().size() == 1;
    }
}
