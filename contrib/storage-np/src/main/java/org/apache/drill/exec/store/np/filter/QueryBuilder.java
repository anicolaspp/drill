package org.apache.drill.exec.store.np.filter;

import org.apache.drill.exec.store.http.filter.ConstantHolder;
import org.apache.drill.exec.store.http.filter.ExprNode;
import org.ojai.store.Connection;
import org.ojai.store.QueryCondition;
import org.ojai.types.ODate;
import org.ojai.types.OTime;
import org.ojai.types.OTimestamp;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

class QueryBuilder {
    
    private Connection connection;
    
    public QueryBuilder(Connection connection) {
        this.connection = connection;
    }
    
    public String getOjaiJsonQueryFrom(ExprNode.AndNode andNode) {
        return createFilterCondition(andNode.children).asJsonString();
    }
    
    private QueryCondition createFilterCondition(List<ExprNode> filters) {
        
        QueryCondition andCondition = connection.newCondition().and();
    
        return filters
                .stream()
                .map(this::evalFilter)
                .reduce(andCondition, QueryCondition::condition)
                .close()
                .build();
    }
    
    private QueryCondition evalFilter(ExprNode filter) {
        if (filter instanceof ExprNode.OrNode) {
            return connection.newCondition()
                    .or()
                    .condition(evalFilter(((ExprNode.OrNode) filter).children.get(0)))
                    .condition(evalFilter(((ExprNode.OrNode) filter).children.get(1)))
                    .close()
                    .build();
        }
        
        if (filter instanceof ExprNode.AndNode) {
            return connection.newCondition()
                    .and()
                    .condition(evalFilter(((ExprNode.AndNode) filter).children.get(0)))
                    .condition(evalFilter(((ExprNode.AndNode) filter).children.get(1)))
                    .close()
                    .build();
        }
    
        if (filter instanceof ExprNode.ColRelOpConstNode) {
            return evalSingleFilter((ExprNode.ColRelOpConstNode) filter);
        }
        
        return connection.newCondition().build();
    }
    
    private QueryCondition evalSingleFilter(ExprNode.ColRelOpConstNode filter) {
        switch (filter.op) {
            case EQ:
                return is(QueryCondition.Op.EQUAL, filter.colName, filter.value).build();
            case NE:
                return is(QueryCondition.Op.NOT_EQUAL, filter.colName, filter.value).build();
            case LT:
                return is(QueryCondition.Op.LESS, filter.colName, filter.value).build();
            case LE:
                return is(QueryCondition.Op.LESS_OR_EQUAL, filter.colName, filter.value).build();
            case GE:
                return is(QueryCondition.Op.GREATER, filter.colName, filter.value).build();
            case GT:
                return is(QueryCondition.Op.GREATER_OR_EQUAL, filter.colName, filter.value).build();
            case IS_NULL:
               return connection.newCondition().notExists(filter.colName).build();
            case IS_NOT_NULL:
                return connection.newCondition().exists(filter.colName).build();
        }
        
        return connection.newCondition().build();
    }
    
    private QueryCondition is(QueryCondition.Op op, String col, ConstantHolder constant) {
        QueryCondition cond = connection.newCondition();
        
        if (col.equals("_id")) {
            return cond.is(col, op, constant.value.toString());
        }
        
        switch (constant.type) {
            case INT:
                return cond.is(col, op, (Integer) constant.value);
            case BIGINT:
                return cond.is(col, op, (Long)constant.value);
            case DECIMAL9:
            case DECIMAL18:
            case DECIMAL28SPARSE:
            case DECIMAL38SPARSE:
                return cond.is(col, op, (Double) constant.value);
            case DATE:
                return cond.is(col, op,  new ODate((Date) constant.value));
            case TIME:
                return cond.is(col, op, new OTime((Time) constant.value));
            case TIMESTAMP:
                return cond.is(col, op, new OTimestamp((Timestamp) constant.value));
            case FLOAT4:
            case FLOAT8:
                return cond.is(col, op, (Float) constant.value);
            case VARCHAR:
            case VAR16CHAR:
            case VARBINARY:
                return cond.is(col, op, constant.value.toString());
        }
        
        return cond;
    }
}
