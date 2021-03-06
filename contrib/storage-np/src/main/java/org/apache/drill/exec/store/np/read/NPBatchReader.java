package org.apache.drill.exec.store.np.read;

import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.store.easy.json.loader.JsonLoader;
import org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl;

import java.io.InputStream;

/**
 * Responsible for read a batch of data from the source
 */
public class NPBatchReader implements ManagedReader<SchemaNegotiator> {

    private JsonLoader jsonLoader;
    private final ReaderProperties properties;


    public NPBatchReader(ReaderProperties props) {
        this.properties = props;
    }

    @Override
    public boolean open(SchemaNegotiator negotiator) {
        RowGenerator dataGen = new OJAIRowGenerator(properties);

        InputStream inStream = dataGen.getRowsInputStream();

        jsonLoader = new JsonLoaderImpl.JsonLoaderBuilder()
                .resultSetLoader(negotiator.build())
                .standardOptions(negotiator.queryOptions())
//                    .dataPath(subScan.tableSpec().connectionConfig().dataPath())
                .fromStream(inStream)
                .build();

        return true;
    }

    @Override
    public boolean next() {
        return jsonLoader.readBatch();
    }

    @Override
    public void close() {
        if (jsonLoader != null) {
            jsonLoader.close();
            jsonLoader = null;
        }
    }

    public ReaderProperties getProperties() {
        return this.properties;
    }
}
