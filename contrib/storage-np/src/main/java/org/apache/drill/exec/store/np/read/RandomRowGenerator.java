package org.apache.drill.exec.store.np.read;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Iterator;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Stream;

/**
 * Generates random rows with a fixed schema.
 *
 * The resulting batch is infinite or we can apply an option limit to it.
 */
public class RandomRowGenerator implements RowGenerator<Row> {

    private final ObjectMapper mapper = new ObjectMapper();
    private final Random random = new Random();

    private final Optional<Integer> limit;

    public RandomRowGenerator() {
        this(0);
    }

    public RandomRowGenerator(Integer limit) {
        if (limit > 0) {
            this.limit = Optional.of(limit);
        } else {
            this.limit = Optional.empty();
        }
    }

    @Override
    public Iterator<Row> getRows() {
        Stream<Row> rows = Stream.generate(this::genNextRandomRow);

        return applyLimit(rows).iterator();
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

    private Row genNextRandomRow() {
        return new Row(
                random.nextInt(10),
                random.nextInt(100),
                String.valueOf(random.nextLong()));
    }

    private Stream<Row> applyLimit(Stream<Row> rows) {
        return limit.map(rows::limit).orElse(rows);
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
