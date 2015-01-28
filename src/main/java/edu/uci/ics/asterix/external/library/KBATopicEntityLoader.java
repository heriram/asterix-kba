package edu.uci.ics.asterix.external.library;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONObject;

import org.apache.commons.io.IOUtils;


public class KBATopicEntityLoader {
    public static final String ENTITY_LOOKUP_FILE = System.getProperty("user.dir") + "/name_variants_lookup.json";

    private static final Logger LOGGER = Logger.getLogger(KBATopicEntityLoader.class.getName());

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
            JSONObject ejson = new JSONObject(IOUtils.toString(is, Charset.forName("utf-8")));

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
            JSONObject ejson = new JSONObject(IOUtils.toString(is, Charset.forName("utf-8")));

            /* Convert json to a list of doc fields - ie., list of stream docs */
            Iterator<String> it = ejson.keys();
            TopicEntity topicEntity = null;
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
