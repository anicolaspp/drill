package org.apache.drill.exec.store.np.ojai;

import org.ojai.store.Connection;

public interface ConnectionProvider {

    default Connection connectTo(String connectionString) {
        return SmartConnectionProvider
                .getInstance()
                .connectTo(connectionString);
    }

}

