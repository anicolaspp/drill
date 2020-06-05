package org.apache.drill.exec.store.np.ojai;

import org.ojai.store.Connection;
import org.ojai.store.DriverManager;

import java.util.HashMap;
import java.util.Map;

/**
 * This class reuses OJAI Connections so they can be shared.
 *
 * OJAI Testing was not designed to keep shared connections so this class bridges that gap.
 * Probably this should be move to OJAI Testing Project.
 */
public class SmartConnectionProvider implements ConnectionProvider {
    private static final SmartConnectionProvider INSTANCE = new SmartConnectionProvider();

    private final Map<String, Connection> registry = new HashMap<>();

    /**
     * Tries to reuse connections whenever possible.
     */
    @Override
    public synchronized Connection connectTo(String connectionString) {
        if (registry.containsKey(connectionString))   {
            return registry.get(connectionString);
        } else {
            Connection newConnection = DriverManager.getConnection(connectionString);

            registry.put(connectionString, newConnection);

            return newConnection;
        }
    }

    public static SmartConnectionProvider getInstance() {
        return INSTANCE;
    }
}
