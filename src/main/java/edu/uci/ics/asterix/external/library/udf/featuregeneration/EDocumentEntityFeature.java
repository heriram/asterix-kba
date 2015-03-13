package edu.uci.ics.asterix.external.library.udf.featuregeneration;

public enum EDocumentEntityFeature {
    MENTIONSTITLE("MentionsTitle"), // # Occurrences in title
    MENTIONSBODY("MentionsBody"), // # Occurrences in body
    MENTIONSANCHOR("MentionsAnchor"), // # Occurrences anchor
    FIRSTPOS("FirstPos"), // First occurrence
    LASTPOS("LastPos"), // Last occurrence
    SPREAD("Spread"), // Distance between first and last occurrences
    FIRSTPOSNORM("FirstPosNorm"),  // normalized first occurrences
    LASTPOSNORM("LastPosNorm"),  // normalized last occurrences
    SPREADNORM("SpreadNorm");  // normalized spread
    
    String name;
    public static final String NUMERIC_TYPE="NUMERIC";
    
    EDocumentEntityFeature(String name) {
        this.name = name;
    }
    
    public String getName() {
        return this.name;
    }
    
    public String getType() {
        return NUMERIC_TYPE;
    }
    
    public static String[] getTypes() {
        String types[] = new String[values().length];
        for (int i=0; i<values().length; i++)
            types[i] = NUMERIC_TYPE;
        
        return types;
    }
    
    public static String[] getNames() {
        String names[] = new String[values().length];
        
        int i=0;
        for (EDocumentEntityFeature f: values()) {
            names[i] = f.name;
            i++;
        }
        return names;
        
    }
}
