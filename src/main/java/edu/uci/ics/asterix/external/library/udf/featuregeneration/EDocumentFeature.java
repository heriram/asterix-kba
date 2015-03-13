package edu.uci.ics.asterix.external.library.udf.featuregeneration;

public enum EDocumentFeature {
    LENGTH_TITLE("LengthTitle", "NUMERIC"),
    LENGTH_BODY("LengthBody", "NUMERIC"),
    LENGTH_ANCHOR("LengthAnchor", "NUMERIC"),
    SOURCE("Source", ESource.getStringOfValues()),
    LANGUAGE("English", "NUMERIC");

    private String fName;
    private String fType;
      
    
    EDocumentFeature(String fName, String fType) {
        this.fName = fName;
        this.fType = fType;
    }

    public static String[] getNames() {
       String names[] = new String[values().length];
       int i=0;
       for (EDocumentFeature f: values()) {
           names[i] = f.fName;
           i++;
       }
       
       return names;
    }
    
    public static String[] getTypes() {
        String types[] = new String[values().length];
        int i=0;
        for (EDocumentFeature f: values()) {
            types[i] = f.fType;
            i++;
        }
        
        return types;
     }
    
    public String getName() {
        return this.fName;
    }
    
    public String getType() {
        return this.fType;
    }
}
