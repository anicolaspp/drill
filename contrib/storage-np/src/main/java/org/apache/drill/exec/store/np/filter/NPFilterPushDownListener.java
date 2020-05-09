package org.apache.drill.exec.store.np.filter;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.store.http.filter.ExprNode;
import org.apache.drill.exec.store.http.filter.ExprNode.ColRelOpConstNode;
import org.apache.drill.exec.store.http.filter.FilterPushDownListener;
import org.apache.drill.exec.store.http.filter.RelOp;
import org.apache.drill.exec.store.np.scan.NPGroupScan;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        return groupScan instanceof NPGroupScan ;
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
    
    static class NPScanPushDownListener implements ScanPushDownListener {
    
        private final NPGroupScan scan;
    
        public NPScanPushDownListener(NPGroupScan scan) {
            this.scan = scan;
        }
        
        @Override
        public ExprNode accept(ExprNode conjunct) {
           if (conjunct instanceof ColRelOpConstNode) {
               ColRelOpConstNode node = (ColRelOpConstNode) conjunct;
               
               return node.op == RelOp.EQ? node : null;
           }
           
           return null;
        }
    
        /**
         * Transform a normalized DNF term into a new scan. Normalized form is:
         * <br><code><pre>
         * (a AND b AND (x OR y))</pre></code><br>
         * In which each {@code OR} term represents a scan partition. It
         * is up to the code here to determine if the scan partition can be handled,
         * corresponds to a storage partition, or can be done as a separate
         * scan (as for a JDBC or REST plugin, say.)
         * <p>
         * Each term is accompanied by the Calcite expression from which it was
         * derived. The caller is responsible for determining which expressions,
         * if any, to leave in the query by returning a list AND'ed (CNF) terms
         * to leave in the query. Those terms can be the ones passed in, or
         * new terms to handle special needs.
         *
         * @param expr
         * @return a pair of elements: a new scan (that represents the pushed filters),
         *                    and the original or new expression to appear in the WHERE clause
         *                    joined by AND with any non-candidate expressions. That is, if analysis
         *                    determines that the plugin can't handle (or cannot completely handle)
         *                    a term, return the Calcite node for that term back as part of the
         *                    return value and it will be left in the query. Any Calcite nodes
         *                    not returned are removed from the query and it is the scan's responsibility
         *                    to handle them. Either the group scan or the list of Calcite nodes
         *                    must be non-null. Or, return null if the filter condition can't be handled
         *                    and the query should remain unchanged.
         */
        @Override
        public Pair<GroupScan, List<RexNode>> transform(ExprNode.AndNode expr) {
            ColRelOpConstNode relOp = (ColRelOpConstNode) expr.children.get(0);
            
            Map<String, String> filters = new HashMap<>();
            
            filters.put(relOp.colName, relOp.value.value.toString());
            
            GroupScan groupScanWithFilters = new NPGroupScan(scan, scan.getColumns(), filters);
            return Pair.of(groupScanWithFilters, Collections.emptyList());
        }
    }
}
