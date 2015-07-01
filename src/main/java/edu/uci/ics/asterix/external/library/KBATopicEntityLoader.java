package edu.uci.ics.asterix.external.library;

import edu.uci.ics.asterix.external.library.textanalysis.ITokenizer;
import edu.uci.ics.asterix.external.library.textanalysis.Tokenizer;
import edu.uci.ics.asterix.external.library.utils.StringUtil;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

//import org.apache.commons.io.IOUtils;

public class KBATopicEntityLoader {
    public static final String ENTITY_LOOKUP_FILE = System.getProperty("user.dir") + "/name_variants_lookup.json";

    private static final Logger LOGGER = Logger.getLogger(KBATopicEntityLoader.class.getName());

    public static String readInputStreamAsString(InputStream in) throws IOException {

        InputStream bis = new BufferedInputStream(in);
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        int result = bis.read();
        while (result != -1) {
            byte b = (byte) result;
            buf.write(b);
            result = bis.read();
        }
        return buf.toString();
    }
    
    private static String getUrlName(String url) throws UnsupportedEncodingException {
        String s[] = url.split("/");
        return URLDecoder.decode(s[s.length - 1].replaceAll("\\(.*\\)", ""), "utf-8");
    }
    
    public static String getAnalyzedName(ITokenizer tokenizer, String name) {
        String tokens[] = tokenizer.tokenize(name);      
        return StringUtil.concatenate(tokens, ' ');
    }
    

    /**
     * Read the topic entities and all related aliases from a JSON file and load them into a list (Map) of
     * name variants
     * 
     * @param pathname
     *            path name of the JSON file containing the name variants
     * @param topicEntities
     *            the map to where the names and the aliases should be loaded
     * @param max
     *            the max number of entities to be read
     */
    public static void loadNameVariants(String pathname, Map<String, Set<String>> topicEntities, int max) {
        try {
            InputStream is = new FileInputStream(pathname);

            // Load to json object
            JSONObject ejson = new JSONObject(readInputStreamAsString(is));

            /* Convert json to a list of doc fields - ie., list of stream docs */
            Iterator<String> it = ejson.keys();
            int n = 0;
            while (it.hasNext() && n < max) {
                String key = it.next();
                String s[] = key.split("/");

                String urlname = URLDecoder.decode(s[s.length - 1].replaceAll("\\(.*\\)", ""), "utf-8"); // Get the url_name only (not the whole URL) 
                String name = urlname.replaceAll("\\(.*\\)", "").replace("_", " ");
                // Get the name variants
                JSONArray nv_array = ejson.getJSONArray(key);
                Set<String> name_variants = new HashSet<String>();
                name_variants.add(name);
                for (int i = 0; i < nv_array.length(); i++) {
                    name_variants.add(nv_array.getString(i).toLowerCase());
                }

                topicEntities.put(urlname, name_variants);
                n++;

            }

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error while loading topic entities and name variants");
            e.printStackTrace();
        }
        log(topicEntities.size() + " topic entities loaded");
    }
    
    public static class TopicEntity {
        private String urlName;
        private Set<String> nameVariants;
        private ITokenizer tokenizer = Tokenizer.INSTANCE;
        
        
        public TopicEntity(String urlName, Set<String> nameVariants) {
            this.urlName = urlName;
            // Save the analyzed version
            this.nameVariants = nameVariants;
           
        }
        
        public ITokenizer getTokenizer() {
            return this.tokenizer;
        }
        
        public void setTokenizer(ITokenizer tokenizer) {
            this.tokenizer = tokenizer;
        }
        
        public Set<String> getNameVariants() {
            return this.nameVariants;
        }
        
        /**
         * Get the analyzed version explicitly if the name variants are not analyzed yet
         *
         * @param analyzedNames
         */
        public void getAnalyzedNameVariants(Set<String> analyzedNames) {
            if (analyzedNames == null) return;
            
            if (!analyzedNames.isEmpty())
                analyzedNames.clear();
            
            for (String name: nameVariants) {
                analyzedNames.add(getAnalyzedName(tokenizer, name));
            }
        }
        
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((urlName == null) ? 0 : urlName.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            
            TopicEntity other = (TopicEntity) obj;
            if (urlName == null) {
                if (other.urlName != null)
                    return false;
            } else if (!urlName.equals(other.urlName))
                return false;
            
            return true;
        }
        
    }
    
    public static void loadTopicEntities(String pathname, Map<String, Set<String>> topicEntities) {
        try {
            InputStream is = new FileInputStream(pathname);
            ITokenizer tokenizer = Tokenizer.INSTANCE;

            // Load to json object
            JSONObject ejson = new JSONObject(readInputStreamAsString(is));

            /* Convert json to a list of doc fields - ie., list of stream docs */
            Iterator<String> it = ejson.keys();
            int n = 0;
            while (it.hasNext()) {
                String key = it.next();
                String urlname = getUrlName(key); // Get the url_name only (not the whole URL) 
                String name = urlname.replaceAll("\\(.*\\)", "").replace("_", " ");
                // Get the name variants
                JSONArray nv_array = ejson.getJSONArray(key);
                Set<String> name_variants = new HashSet<String>();
                name_variants.add(getAnalyzedName(tokenizer, name));
                for (int i = 0; i < nv_array.length(); i++) {
                    name_variants.add(getAnalyzedName(tokenizer, nv_array.getString(i)));
                }

                topicEntities.put(urlname, name_variants);
                n++;

            }

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error while loading topic entities and name variants");
            e.printStackTrace();
        }
        log(topicEntities.size() + " topic entities loaded");
    }
    
    
    public static void loadTopicEntities(Map<String, Set<String>> topicEntities) {
        loadTopicEntities(ENTITY_LOOKUP_FILE, topicEntities);
    }
    

    private static void addSynonyms(Map<String,Set<String>> topicEntitySynMap, Set<String> variants) {
        
        Set<String> syns;
        for (String variant1: variants) {
            if (topicEntitySynMap.containsKey(variant1))
                syns = topicEntitySynMap.get(variant1);
            else syns = new HashSet<String>();
            for (String variant2: variants) {
                if (variant2 != variant1)
                    syns.add(variant2);
            }
            topicEntitySynMap.put(variant1, syns);
        }

    }
    
    
    public static void buildNameURLMap(Map<String, String> topicEntityURLMap) {
        buildNameURLMap(ENTITY_LOOKUP_FILE, topicEntityURLMap);
    }
    
    public static void buildNameURLMap(String pathname, Map<String, String> topicEntityURLMap) {
        ITokenizer tokenizer = Tokenizer.INSTANCE;
        try {
            JSONObject ejson = readJSONFile(pathname);
            /* Convert json to a list of doc fields - ie., list of stream docs */
            Iterator<String> it = ejson.keys();
            while (it.hasNext()) {
                String key = it.next();
                String urlname = getUrlName(key);
                String name = getAnalyzedName(tokenizer, urlname.replace("_", " "));  

                // Get the name variants
                JSONArray nv_array = ejson.getJSONArray(key);
                topicEntityURLMap.put(getAnalyzedName(tokenizer, name), urlname);
                
                for (int i = 0; i < nv_array.length(); i++) {
                    String nv = nv_array.getString(i);
                    topicEntityURLMap.put(getAnalyzedName(tokenizer, nv), urlname);
                }

                
            }

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error while loading topic entities and name variants");
            e.printStackTrace();
        }
    }
    
    private static JSONObject readJSONFile(String pathname) throws JSONException, IOException {
        InputStream is = new FileInputStream(pathname);
        // Load to json object
        return new JSONObject(readInputStreamAsString(is));
    }
    
    public static void buildNameSynonymMap(Map<String, Set<String>> topicEntitySynMap) {
        buildNameSynonymMap(ENTITY_LOOKUP_FILE, topicEntitySynMap);
    }

    public static void buildNameSynonymMap(String pathname, Map<String, Set<String>> topicEntitySynMap) {
        try {
            InputStream is = new FileInputStream(pathname);

            // Load to json object
            JSONObject ejson = new JSONObject(readInputStreamAsString(is));

            /* Convert json to a list of doc fields - ie., list of stream docs */
            Iterator<String> it = ejson.keys();
            while (it.hasNext()) {
                String key = it.next();
                String s[] = key.split("/");

                String urlname = URLDecoder.decode(s[s.length - 1].replaceAll("\\(.*\\)", "").replace("_", " "),
                        "utf-8").toLowerCase(); // Get the url_name only (not the whole URL) 

                // Get the name variants
                JSONArray nv_array = ejson.getJSONArray(key);
                Set<String> name_variants = new HashSet<String>();
                name_variants.add(urlname.intern());
                for (int i = 0; i < nv_array.length(); i++) {
                    name_variants.add(nv_array.getString(i).toLowerCase().intern());
                }

                addSynonyms(topicEntitySynMap, name_variants);
            }

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error while loading topic entities and name variants");
            e.printStackTrace();
        }
    }

    private static void log(String msg) {
        if (LOGGER.isLoggable(Level.INFO))
            LOGGER.info(msg);

    }

    /**
     * Read the topic entities and all related aliases from a JSON file and load them into a list (Set) of
     * name variants
     * 
     * @param pathname
     *            path name of the JSON file containing the name variants
     * @param max
     *            the max number of entities to be read
     */
    public static void loadNameVariants(String pathname, Set<String> name_variants, int max) {
        try {
            InputStream is = new FileInputStream(pathname);

            // Load to json object
            //JSONObject ejson = new JSONObject(IOUtils.toString(is, Charset.forName("utf-8")));
            JSONObject ejson = new JSONObject(readInputStreamAsString(is));

            /* Convert json to a list of doc fields - ie., list of stream docs */
            Iterator<String> it = ejson.keys();
            int n = 0;
            while (it.hasNext() && n < max) {
                String key = it.next();

                String s[] = key.split("/");
                String name = (s[s.length - 1]).replaceAll("\\(.*\\)", "").replace("_", " ");
                name_variants.add(URLDecoder.decode(name, "utf-8").toLowerCase());
                JSONArray nv_array = ejson.getJSONArray(key);
                // Get the name variants
                for (int i = 0; i < nv_array.length(); i++) {
                    name_variants.add(nv_array.getString(i).toLowerCase());
                    n++;
                }
            }

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error while loading topic entities and name variants");
            e.printStackTrace();
        }

        log(name_variants.size() + " name variants loaded");

    }

    public static String[][] loadAnalyzedNameVariants(ITokenizer tokenizer, String pathname, int max) {
        Set<String> nv = new HashSet<String>();
        loadNameVariants(pathname, nv, max);
        Iterator<String> it = nv.iterator();
        String[][] nameVariants = new String[nv.size()][];
        int i = 0;
        while (it.hasNext()) {
            nameVariants[i] = tokenizer.tokenize(it.next());
            i++;
        }
        return nameVariants;
    }

    /**
     * Read all topic entities and all related aliases from a JSON file (default file)
     * and load them into a list (set) of name variants - pre-analyze the names for performance
     *
     * @return String[][]
     */

    public static String[][] loadNameVariants(ITokenizer tokenizer) {
        return loadAnalyzedNameVariants(tokenizer, ENTITY_LOOKUP_FILE, Integer.MAX_VALUE);
    }

    /**
     * Read all topic entities and all related aliases from a JSON file (default file)
     * and load them into a list (set) of name variants
     * 
     * @param name_variants
     *            the set to where the names variants will be loaded
     */
    public static void loadNameVariants(Set<String> name_variants) {
        loadNameVariants(ENTITY_LOOKUP_FILE, name_variants, Integer.MAX_VALUE);
    }
    


}
