package org.apache.drill.exec.store.np.filter;

import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.store.http.filter.FilterPushDownListener;
import org.apache.drill.exec.store.np.scan.NPGroupScan;

public class NPFilterPushDownListener implements FilterPushDownListener {
    
    private boolean hasBeenApplied = false;
    
    /**
     * @return a prefix to display in filter rules
     */
    @Override
    public String prefix() {
        return "NP";
    }
    
    /**
     * Broad check to see if the scan is of the correct type for this
     * listener. Generally implemented as: <code><pre>
     * public boolean isTargetScan(ScanPrel scan) {
     *   return scan.getGroupScan() instanceof MyGroupScan;
     * }
     * </pre></code>
     *
     * @param groupScan
     * @return true if the given group scan is one this listener can
     * handle, false otherwise
     */
    @Override
    public boolean isTargetScan(GroupScan groupScan) {
        return groupScan instanceof NPGroupScan;
    }
    
    /**
     * Check if the filter rule should be applied to the target group scan,
     * and if so, return the builder to use.
     * <p>
     * Calcite will run this rule multiple times during planning, but the
     * transform only needs to occur once.
     * Allows the group scan to mark in its own way whether the rule has
     * been applied.
     *
     * @param groupScan the scan node
     * @return builder instance if the push-down should be applied,
     * null otherwise
     */
    @Override
    public ScanPushDownListener builderFor(GroupScan groupScan) {
        NPGroupScan scan = (NPGroupScan) groupScan;
        
        if (hasBeenApplied) {
            return null;
        } else {
            hasBeenApplied = true;
            
            return new NPScanPushDownListener(scan);
        }
    }
}

