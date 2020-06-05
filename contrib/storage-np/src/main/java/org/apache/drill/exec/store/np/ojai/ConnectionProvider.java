package org.apache.drill.exec.store.np.ojai;

import org.ojai.store.Connection;
import org.ojai.store.DriverManager;

/**
 * Easy access to OJAI Connections
 */
public interface ConnectionProvider {
    
    /**
     * Get a handle to a OJAI Connection.
     * <p>
     * Depending of the cache policy configured through {@code shouldCacheConnections} it might return a new connection
     * or reuse a previously used one.
     *
     * @param connectionString Connection string to be used. The connection string has information about the context
     *                         being used by OJAI.
     * @return OJAI Connection.
     */
    default Connection connectTo(String connectionString) {
        if (shouldCacheConnections()) {
            return SmartConnectionProvider
                    .getInstance()
                    .connectTo(connectionString);
        } else {
            return DriverManager.getConnection(connectionString);
        }
    }
    
    /**
     * Indicates if connections should be reuse.
     *
     * @return true if connections should be reuse; false otherwise.
     */
    default boolean shouldCacheConnections() {
        return true;
    }
    
}

