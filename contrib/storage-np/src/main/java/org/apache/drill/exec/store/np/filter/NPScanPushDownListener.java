package org.apache.drill.exec.store.np.filter;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.store.http.filter.ExprNode;
import org.apache.drill.exec.store.http.filter.FilterPushDownListener;
import org.apache.drill.exec.store.np.ojai.ConnectionProvider;
import org.apache.drill.exec.store.np.scan.NPGroupScan;
import org.ojai.store.Connection;

import java.util.Collections;
import java.util.List;




class NPScanPushDownListener implements FilterPushDownListener.ScanPushDownListener, ConnectionProvider {
    
    private final NPGroupScan scan;
    private final Connection connection;
    
    public NPScanPushDownListener(NPGroupScan scan) {
        this.scan = scan;
        this.connection = connectTo(scan.getScanSpec().getPluginConfig().getConnection());
    }
    
    @Override
    public ExprNode accept(ExprNode conjunct) {
        if (isSupportedFilter(conjunct)) {
            return conjunct;
        } else {
            return null;
        }
    }
    
    @Override
    public Pair<GroupScan, List<RexNode>> transform(ExprNode.AndNode andNode) {
        double selectivity = 1;
        
        QueryBuilder ojaiQueryBuilder = new QueryBuilder(connection);
        String ojaiJsonQuery = ojaiQueryBuilder.getOjaiJsonQueryFrom(andNode);
    
        System.out.println(ojaiJsonQuery);
        
        GroupScan groupScanWithFilters = new NPGroupScan(scan, scan.columns(), ojaiJsonQuery);
        return Pair.of(groupScanWithFilters, Collections.emptyList());
    }
    
    private boolean isSupportedFilter(ExprNode node) {
        if (node instanceof ExprNode.AndNode) {
            ExprNode.AndNode andNode = (ExprNode.AndNode) node;
            
            return andNode.children.stream().allMatch(this::isSupportedFilter);
        }
        
        if (node instanceof ExprNode.OrNode) {
            ExprNode.OrNode orNode = (ExprNode.OrNode) node;
            
            return orNode.children.stream().anyMatch(this::isSupportedFilter);
        }
        
        if (node instanceof ExprNode.ColRelOpConstNode) {
            ExprNode.ColRelOpConstNode singleExp = (ExprNode.ColRelOpConstNode) node;
            
            switch (singleExp.op) {
                case EQ:
                case NE:
                case LT:
                case LE:
                case GE:
                case GT:
                case IS_NULL:
                case IS_NOT_NULL:
                    return isSupportedType(singleExp.value.type);
            }
        }
        
        return false;
    }
    
    private boolean isSupportedType(TypeProtos.MinorType type) {
        switch (type) {
            case INT:
            case BIGINT:
            case DECIMAL9:
            case DECIMAL18:
            case DECIMAL28SPARSE:
            case DECIMAL38SPARSE:
            case DATE:
            case TIME:
            case TIMESTAMP:
            case FLOAT4:
            case FLOAT8:
            case VARCHAR:
            case VAR16CHAR:
            case VARBINARY:
            case NULL:
                return true;
        }
        
        return false;
    }
}


