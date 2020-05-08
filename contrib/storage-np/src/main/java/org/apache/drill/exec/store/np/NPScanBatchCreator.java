package org.apache.drill.exec.store.np;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedScanFramework;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.easy.json.loader.JsonLoader;
import org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl;
import org.apache.drill.exec.store.np.scan.NPSubScan;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.stream.Stream;

public class NPScanBatchCreator implements BatchCreator<NPSubScan> {
    
    @Override
    public CloseableRecordBatch getBatch(ExecutorFragmentContext context, NPSubScan subScan, List<RecordBatch> children) throws ExecutionSetupException {
        Preconditions.checkArgument(children.isEmpty());
        
        
        return createBuilder(context.getOptions(), subScan)
                .buildScanOperator(context, subScan);
    }
    
    private ManagedScanFramework.ScanFrameworkBuilder createBuilder(OptionManager optionValues,
                                                                    NPSubScan subScan) {
        
        ManagedScanFramework.ScanFrameworkBuilder builder = new ManagedScanFramework.ScanFrameworkBuilder();
        builder.projection(subScan.getColumns());
        builder.setUserName(subScan.getUserName());
        
        ManagedScanFramework.ReaderFactory readerFactory = new NPReaderFactory(subScan);
        builder.setReaderFactory(readerFactory);
        builder.nullType(Types.optional(TypeProtos.MinorType.VARCHAR));
        
        return builder;
    }
    
    private static class NPReaderFactory implements ManagedScanFramework.ReaderFactory {
        
        private NPSubScan subScan;
        
        private NPBatchReader reader;
        
        int numberOfReader = 1;
    
        public NPReaderFactory(NPSubScan subScan) {
            this.subScan = subScan;
            this.reader = new NPBatchReader(subScan);
        }
    
        @Override
        public void bind(ManagedScanFramework framework) {
        
        }
        
        @Override
        public ManagedReader<? extends SchemaNegotiator> next() {
            if (numberOfReader <= 0) {
                return null;
            }
            
            numberOfReader--;
            
            return reader;
        }
    }
    
    static class NPBatchReader implements ManagedReader<SchemaNegotiator> {
        
        private NPSubScan subScan;
        
        private JsonLoader jsonLoader;
        
        int count = 0;
        
        public NPBatchReader(NPSubScan subScan) {
            this.subScan = subScan;
        }
        
        /**
         * Setup the record reader. Called just before the first call
         * to <tt>next()</tt>. Allocate resources here, not in the constructor.
         * Example: open files, allocate buffers, etc.
         *
         * @param negotiator mechanism to negotiate select and table
         *                   schemas, then create the row set reader used to load data into
         *                   value vectors
         * @return true if the reader is open and ready to read (possibly no)
         * rows. false for a "soft" failure in which no schema or data is available,
         * but the scanner should not fail, it should move onto another reader
         * @throws RuntimeException for "hard" errors that should terminate
         *                          the query. <tt>UserException</tt> preferred to explain the problem
         *                          better than the scan operator can by guessing at the cause
         */
        @Override
        public boolean open(SchemaNegotiator negotiator) {
            RowGenerator dataGen = new RowGenerator();
            
            InputStream inStream = dataGen.getRowsInputStream();
            
            jsonLoader = new JsonLoaderImpl.JsonLoaderBuilder()
                    .resultSetLoader(negotiator.build())
                    .standardOptions(negotiator.queryOptions())
//                    .dataPath(subScan.tableSpec().connectionConfig().dataPath())
                    .fromStream(inStream)
                    .build();
            
            return true;
        }
        
        /**
         * Read the next batch. Reading continues until either EOF,
         * or until the mutator indicates that the batch is full.
         * The batch is considered valid if it is non-empty. Returning
         * <tt>true</tt> with an empty batch is valid, and is helpful on
         * the very first batch (returning schema only.) An empty batch
         * with a <tt>false</tt> return code indicates EOF and the batch
         * will be discarded. A non-empty batch along with a <tt>false</tt>
         * return result indicates a final, valid batch, but that EOF was
         * reached and no more data is available.
         * <p>
         * This somewhat complex protocol avoids the need to allocate a
         * final batch just to find out that no more data is available;
         * it allows EOF to be returned along with the final batch.
         *
         * @return <tt>true</tt> if more data may be available (and so
         * <tt>next()</tt> should be called again, <tt>false</tt> to indicate
         * that EOF was reached
         * @throws RuntimeException (<tt>UserException</tt> preferred) if an
         *                          error occurs that should fail the query.
         */
        @Override
        public boolean next() {
            System.out.println("NPBatchReader:next invoked...");
            
            return jsonLoader.readBatch();
        }
        
        /**
         * Release resources. Called just after a failure, when the scanner
         * is cancelled, or after <tt>next()</tt> returns EOF. Release
         * all resources and close files. Guaranteed to be called if
         * <tt>open()</tt> returns normally; will not be called if <tt>open()</tt>
         * throws an exception.
         *
         * @throws RuntimeException (<tt>UserException</tt> preferred) if an
         *                          error occurs that should fail the query.
         */
        @Override
        public void close() {
            if (jsonLoader != null) {
                jsonLoader.close();
                jsonLoader = null;
            }
        }
        
        
    }
    
    static class RowGenerator {
        
        ObjectMapper mapper = new ObjectMapper();
        Random random = new Random();
        
        private String genNextRandomJsonRow() {
            Row row = new Row(
                    random.nextInt(10),
                    random.nextInt(100),
                    String.valueOf(random.nextLong()));
            
            try {
                return mapper.writeValueAsString(row);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
                
                return "";
            }
        }
        
        public Iterator<String> getRows() {
            return Stream
                    .generate(this::genNextRandomJsonRow)
                    .limit(10)
                    .iterator();
        }
        
        public InputStream getRowsInputStream() {
            Iterator<String> rows = getRows();
            
            return new InputStream() {
                final Queue<Byte> buffer = new ArrayDeque<>();
                
                @Override
                public int read() throws IOException {
                    if (!buffer.isEmpty()) {
                        return buffer.poll();
                    }
                    
                    if (rows.hasNext()) {
                        enQueueAll(rows.next().getBytes());
                        
                        return read();
                    }
                    
                    return -1;
                }
                
                private void enQueueAll(byte[] bytes) {
                    for (byte aByte : bytes) {
                        buffer.add(aByte);
                    }
                }
            };
        }
        
    }
    
    @JsonTypeName("np-row")
    static class Row {
        private final Integer a;
        private final Integer b;
        private final String c;
        
        @JsonCreator
        public Row(Integer a, Integer b, String c) {
            this.a = a;
            this.b = b;
            this.c = c;
        }
        
        @JsonProperty("a")
        public Integer getA() {
            return a;
        }
        
        @JsonProperty("b")
        public Integer getB() {
            return b;
        }
        
        @JsonProperty("c")
        public String getC() {
            return c;
        }
    }
}
