package edu.uci.ics.asterix.external.library.udf.featuregeneration;

public interface IFeatureProperty {
    public void setProp(String key, Object value);
    public Object getProp(String key);
    
}
