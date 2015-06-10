package edu.uci.ics.asterix.external.library.statistics;

import java.util.List;

import org.apache.commons.math3.stat.descriptive.moment.Kurtosis;

public class EntityKurtosis extends TemporalStatistics<String, Double> {
    private final Kurtosis kurtosis = new Kurtosis();
    
    public final static EntityKurtosis INSTANCE = new EntityKurtosis();
    
    private EntityKurtosis() {
        super();
    }
    
    @Override
    public double evaluate(String key) {
        double[] data = getDoubleValuesAsArray(key);
        return kurtosis.evaluate(data);
    }

    @Override
    public List<Double> computeValue(List<String> temporalValues) {
        // TODO Auto-generated method stub
        return null;
    }

}
