package org.apache.drill.exec.store.np.filter;

import org.apache.drill.exec.store.http.filter.RelOp;

public class Filter {
    private String column;
    private RelOp op;
    private Object value;
    
    Filter(String column, RelOp op, Object value) {
        this.column = column;
        this.op = op;
        this.value = value;
    }
    
    public String getColumn() {
        return column;
    }
    
    public RelOp getOp() {
        return op;
    }
    
    public Object getValue() {
        return value;
    }
}
