package edu.uci.ics.asterix.external.library;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.analysis.Analyzer;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.asterix.external.library.utils.TextAnalysis;

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
            TopicEntity topicEntity = null;
            int n = 0;
            while (it.hasNext() && n < max) {
                String key = it.next();
                String s[] = key.split("/");

                String urlname = s[s.length - 1]; // Get the url_name only (not the whole URL) 

                // Get the name variants
                JSONArray nv_array = ejson.getJSONArray(key);
                Set<String> name_variants = new HashSet<String>();
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
        try {
            JSONObject ejson = readJSONFile(pathname);
            /* Convert json to a list of doc fields - ie., list of stream docs */
            Iterator<String> it = ejson.keys();
            while (it.hasNext()) {
                String key = it.next();
                String s[] = key.split("/");

                // Get the url_name only (not the whole URL)
                String urlname = URLDecoder.decode(s[s.length - 1].replaceAll("\\(.*\\)", ""), "utf-8");
                String name = urlname.replace("_", " ").toLowerCase();  

                // Get the name variants
                JSONArray nv_array = ejson.getJSONArray(key);
                topicEntityURLMap.put(name, key);
                
                for (int i = 0; i < nv_array.length(); i++) {
                    String nv = nv_array.getString(i).toLowerCase();
                    topicEntityURLMap.put(nv, key);
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
     * @param topicEntities
     *            the map to where the names and the aliases should be loaded
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

    public static String[][] loadAnalyzedNameVariants(Analyzer analyzer, String pathname, int max) {
        Set<String> nv = new HashSet<String>();
        loadNameVariants(pathname, nv, max);
        Iterator<String> it = nv.iterator();
        String[][] nameVariants = new String[nv.size()][];
        int i = 0;
        while (it.hasNext()) {
            nameVariants[i] = TextAnalysis.analyze(analyzer, it.next());
            i++;
        }
        return nameVariants;
    }

    /**
     * Read all topic entities and all related aliases from a JSON file (default file)
     * and load them into a list (set) of name variants - pre-analyze the names for performance
     * 
     * @param analyzer
     * @return String[][]
     */

    public static String[][] loadNameVariants(Analyzer analyzer) {
        return loadAnalyzedNameVariants(analyzer, ENTITY_LOOKUP_FILE, Integer.MAX_VALUE);
    }

    /**
     * Read all topic entities and all related aliases from a JSON file (default file)
     * and load them into a list (set) of name variants
     * 
     * @param topicEntities
     *            the set to where the names variants will be loaded
     */
    public static void loadNameVariants(Set<String> name_variants) {
        loadNameVariants(ENTITY_LOOKUP_FILE, name_variants, Integer.MAX_VALUE);
    }
    


}
