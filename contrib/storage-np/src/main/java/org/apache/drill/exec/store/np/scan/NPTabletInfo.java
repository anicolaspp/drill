package org.apache.drill.exec.store.np.scan;

import java.util.List;

public class NPTabletInfo {
    private final String filter;
    private final List<String> locations;

    public NPTabletInfo(String filter, List<String> locations) {
        this.filter = filter;
        this.locations = locations;
    }

    public String getFilter() {
        return this.filter;
    }

    public List<String> getLocations() {
        return locations;
    }
}
