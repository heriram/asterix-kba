package edu.uci.ics.asterix.external.library.statistics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public abstract class  TemporalStatistics<K,V> {
    Map<K,List<V>> dataMap;
    Map<K,List<String>> temporalMap;
    
    protected TemporalStatistics() {
        dataMap = new HashMap<>();
        temporalMap = new HashMap<>();
    }
    
    public List<V> getDataValues(K key) {
        List<V> dataValues = dataMap.get(key);      
        return dataValues;
    }
    
    @SuppressWarnings("unchecked")
    protected V[] getValuesAsArray(K key) {
        List<V> dataValues = dataMap.get(key);
        V[] valuesAsArray = (V[])dataValues.toArray(new Object[dataValues.size()]);      
        return valuesAsArray;
    }
    
    @SuppressWarnings("unchecked")
    protected double[] getDoubleValuesAsArray(K key) {
        List<V> dataValues = dataMap.get(key);
        double values[] = new double[dataValues.size()];
        if (!(dataValues.get(0) instanceof Double)) {
            try {
                throw new Exception("Unsuported type.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        int i=0;
        for (V value: dataValues) {
            values[i] = ((Double) value).doubleValue();
        }
        return values;
    }
    
    
    public void addValue(K key, V value) {
        List<V> dataValues = null;

        if (dataMap.containsKey(key))
            dataValues = dataMap.get(key);
        else dataValues = new ArrayList<V>();
        
        dataValues.add(value);
        dataMap.put(key, dataValues);
    }
    
    public void addTime(K key, String value) {
        List<String> temporalValues = null;

        if (temporalMap.containsKey(key))
            temporalValues = temporalMap.get(key);
        else temporalValues = new ArrayList<String>();
        
        temporalValues.add(value);
        temporalMap.put(key, temporalValues);
    }
    
    public void buildDataValue() {
        Set<Entry<K, List<String>>> entySet = temporalMap.entrySet();
        Iterator<Entry<K, List<String>>> it = entySet.iterator();
        
        K key = null;
        List<String> values = null;
        while(it.hasNext()) {
            Entry<K, List<String>> e = it.next();
            values = e.getValue();
            key = e.getKey();
            List<V> dataValues = computeValue(values);
            dataMap.put(key, dataValues);            
        }
    }
    
    public List<V> getValues(K key) {
        return dataMap.get(key);
    }
    
    public List<String> getTimes(K key) {
        return temporalMap.get(key);
    }
    
    abstract public List<V> computeValue(List<String> temporalValues);
    
    abstract public double evaluate(K key);
    
    
}
