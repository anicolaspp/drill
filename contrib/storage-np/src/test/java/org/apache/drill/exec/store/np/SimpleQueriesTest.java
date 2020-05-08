package org.apache.drill.exec.store.np;

import com.github.anicolaspp.ojai.JavaOjaiTesting;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.rowSet.DirectRowSet;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.np.ojai.SmartConnectionProvider;
import org.apache.drill.test.BaseDirTestWatcher;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryBuilder;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.ojai.Document;
import org.ojai.store.Connection;
import org.ojai.store.DocumentStore;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * For testing wild card test against in-memory store
 */
public class SimpleQueriesTest extends ClusterTest {
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
        String sql = "SELECT * FROM np.`/user/store1` ORDER BY value LIMIT 2";

        DirectRowSet result = client.queryBuilder().sql(sql).rowSet();

        TupleMetadata expectedSchema = new SchemaBuilder()
                .add("_id", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
                .add("value", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
                .buildSchema();

        RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
                .addRow("0", 0)
                .addRow("1", 1)
                .build();

        RowSetUtilities.verify(expected, result);
    }

    @Test
    public void testReadFromValidTables() throws Exception {
        String sql = "SELECT * FROM np.`/non-existing-table`";

        try {
            client.queryBuilder().sql(sql).run();

            fail();
        } catch (UserRemoteException e) {
            String msg = e.getMessage();

            assert e.getErrorType() == UserBitShared.DrillPBError.ErrorType.VALIDATION;
            assertTrue(msg.contains("'/non-existing-table' not found within 'np'"));
        }
    }

    @Test
    public void testSelfJoin() throws Exception {
        String sql =
                "SELECT a._id, b.value " +
                        "FROM np.`/user/store1` a inner join np.`/user/store1` b " +
                        "ON a._id = b._id "+
                        "WHERE a.value = 5";

        TupleMetadata expectedSchema = new SchemaBuilder()
                .add("_id", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
                .add("value", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
                .buildSchema();

        RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
                .addRow("5", 5)
                .build();

        DirectRowSet result = client.queryBuilder().sql(sql).rowSet();

        RowSetUtilities.verify(expected, result);
    }
}
