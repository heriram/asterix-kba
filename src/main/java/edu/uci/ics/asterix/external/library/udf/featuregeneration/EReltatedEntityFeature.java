package edu.uci.ics.asterix.external.library.udf.featuregeneration;

public enum EReltatedEntityFeature implements IFeatureEnum {
    RELATED("Related"),
    RELATEDTITLE("RelatedTitle"),
    RELATEDBODY("RelatedBody"),
    RELATEDANCHOR("RelatedAnchor");

    public static final String NUMERIC_TYPE = "NUMERIC";
    
    private String fName;

    EReltatedEntityFeature(String name) {
        this.fName = name;
    }

    @Override
    public String getName() {
        return this.fName;
    }

    @Override
    public String getType() {
        return NUMERIC_TYPE;
    }
    
    public static String[] getNames() {
        String names[] = new String[values().length];
        int i=0;
        for (EReltatedEntityFeature f: values()) {
            names[i] = f.fName;
            i++;
        }
        
        return names;
     }
     
     public static String[] getTypes() {
         String types[] = new String[values().length];
         int i=0;
         for (EReltatedEntityFeature f: values()) {
             types[i] = NUMERIC_TYPE;
             i++;
         }
         
         return types;
      }
   

}
