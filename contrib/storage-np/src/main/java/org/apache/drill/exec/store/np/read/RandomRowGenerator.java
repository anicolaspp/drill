package org.apache.drill.exec.store.np.read;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Iterator;
import java.util.Random;
import java.util.stream.Stream;

public class RandomRowGenerator implements RowGenerator<Row> {
    
    ObjectMapper mapper = new ObjectMapper();
    Random random = new Random();
    
    private Row genNextRandomRow() {
        return new Row(
                random.nextInt(10),
                random.nextInt(100),
                String.valueOf(random.nextLong()));
    }
    
    @Override
    public Iterator<Row> getRows() {
        return Stream
                .generate(this::genNextRandomRow)
                .limit(10)
                .iterator();
    }
    
    @Override
    public byte[] getBytesFrom(Row value) {
        try {
            return mapper.writeValueAsString(value).getBytes();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            
            return new byte[0];
        }
    }
}

@JsonTypeName("np-row")
class Row {
    private final Integer a;
    private final Integer b;
    private final String c;
    
    @JsonCreator
    public Row(Integer a, Integer b, String c) {
        this.a = a;
        this.b = b;
        this.c = c;
    }
    
    @JsonProperty("a")
    public Integer getA() {
        return a;
    }
    
    @JsonProperty("b")
    public Integer getB() {
        return b;
    }
    
    @JsonProperty("c")
    public String getC() {
        return c;
    }
}
