package edu.uci.ics.asterix.external.library.udf.featuregeneration;

public enum ESource {
    news(0),
    MAINSTREAM_NEWS(0),
    SOCIAL(1),
    social(1),
    arxiv(2),
    FORUM(3),
    linking(4), 
    MEMETRACKER(5),
    REVIEW(6),
    WEBLOG(7),
    CLASSIFIED(8);
    
    private int value;
    
    ESource(int val) {
        this.value = val;
    }
    
    public static int[] getValues() {
        int values[] = new int[values().length];
        int i=0;
        for (ESource source: values())
            values[i] = source.value;
        
        return values;
    }
    
    public static String getStringOfValues() {
        StringBuilder sb = new StringBuilder();
        int values[] = new int[values().length];
        
        for (ESource source: values()) {
            values[source.value] = source.value; 
        }
        sb.append("{" + values[0]);
        for ( int i=1; i<values.length && values[i]>=i; i++)
                sb.append(", " + values[i]);
        sb.append('}');
        
        return sb.toString();
    }
    
    public static int getValueOfName(String name) {
        return valueOf(name).value;
    }
    
    public int getValue() {
        return this.value;
    }
}
