package org.apache.drill.exec.store.np.read;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;

/**
 * Generic interface for generating Row based data
 *
 * @param <A>
 */
public interface RowGenerator<A> {
    /**
     * Iterator of rows
     *
     * @return
     */
    Iterator<A> getRows();
    
    /**
     * Functor Row -> byte[]
     *
     * @param value
     * @return
     */
    byte[] getBytesFrom(A value);
    
    /**
     * Lazy input stream where the collection of rows (Iterator[byte[]]) is written
     *
     * @return
     */
    default InputStream getRowsInputStream() {
        Iterator<A> rows = getRows();
        
        return new InputStream() {
            final Queue<Byte> buffer = new ArrayDeque<>();
            
            @Override
            public int read() throws IOException {
                if (!buffer.isEmpty()) {
                    return buffer.poll();
                }
                
                if (rows.hasNext()) {
                    enQueueAll(getBytesFrom(rows.next()));
                    
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
