package state;

/**
 * This is a wrapper for a java Integer, used by {@link IntegerAccumulator}
 */
public class IntegerAggregate {
    private Integer value;

    public IntegerAggregate() {
        this(0);
    }

    public IntegerAggregate(Integer value) {
        this.value = value;
    }

    public Integer getValue() {
        return value;
    }

    public void setValue(Integer value) {
        this.value = value;
    }

}
