package org.apache.drill.exec.store.np;

import com.github.anicolaspp.ojai.JavaOjaiTesting;
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

import java.util.Iterator;

/**
 * This class tests that our plugin loads table using the defined indexing architecture.
 *
 * One a single index is use in this suit.
 */
public class SingleIndexQueryTests extends ClusterTest {
    @ClassRule
    public static final BaseDirTestWatcher dirTestWatcher = new BaseDirTestWatcher();

    private static final JavaOjaiTesting ojai = new JavaOjaiTesting();

    @BeforeClass
    public static void setup() throws Exception {
        ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));

        defineNPPlugin();

        genDataMainTable();
        genIndexes();
    }

    @SuppressWarnings("source")
    private static void defineNPPlugin() throws StoragePluginRegistry.PluginException {
        Drillbit drillbit = cluster.drillbit();

        NPStoragePluginConfig config = new NPStoragePluginConfig("ojai:anicolaspp:mem");
        config.setEnabled(true);

        StoragePluginRegistry pluginRegistry = drillbit.getContext().getStorage();
        pluginRegistry.put(NPStoragePluginConfig.NAME, config);
    }

    public static void genDataMainTable() {

        Connection connection = SmartConnectionProvider
                .getInstance()
                .connectTo("ojai:anicolaspp:mem");

        DocumentStore store = connection.getStore("/user/main");

        for (int i = 0; i < 100; i++) {
            Document doc = ojai
                    .getConnection()
                    .newDocument()
                    .setId(String.valueOf(i))
                    .set("value", i);

            store.insert(doc);
        }
    }

    public static void genIndexes() {
        Connection connection = SmartConnectionProvider
                .getInstance()
                .connectTo("ojai:anicolaspp:mem");

        DocumentStore store = connection.getStore("/user/main");

        DocumentStore index = connection.getStore("/user/main_value");

        Iterator<Document> docs = store.find().iterator();

        int seqId = 0;

        while (docs.hasNext()) {
            Document current = docs.next();

            String indexedId = current.getInt("value") + "_" + seqId++;

            Document indexed = connection
                    .newDocument()
                    .setId(indexedId)
                    .set("ref", current.getIdString());

            index.insert(indexed);
        }
    }

    @Test
    public void testFilterPushDownToIndex() throws Exception {
//        String sql = "SELECT value FROM np.`/user/main` WHERE (value = 5 AND _id > 5) OR _id = 1";
        String sql = "SELECT value FROM np.`/user/main`";
    
        QueryBuilder builder = client.queryBuilder().sql(sql);
    
        builder.printCsv();
        
        assert builder.rowSet().rowCount() == 2;
    }
}
