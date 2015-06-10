package edu.uci.ics.asterix.external.library.statistics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.uci.ics.asterix.external.library.KBATopicEntityLoader;

public class MentionCounter {
    public final static Map<String, String> ENTITY_URL_MAP = new HashMap<String, String>(); // Mapping between url_name and an entity name variant
    static {
        KBATopicEntityLoader.buildNameURLMap(ENTITY_URL_MAP);
    }

    private Map<String, List<Double>> frequencyMap; // The mention frequency hash table
    private Map<String, Integer> timeIndexMap; // Time index hash table
    
    public MentionCounter INSTANCE = new MentionCounter();
    
    private MentionCounter() {
        this.frequencyMap = new HashMap<>();
        this.timeIndexMap = new HashMap<>();
    }
    
    private String getDate(String dirName) {
        int endIndex = dirName.lastIndexOf('-');
        return dirName.substring(0, endIndex);
    }
    
    public void addMentionNumbers(String name, String dirName, double mentions) {
        String date = getDate(dirName);
        double f = mentions;

        String urlName = ENTITY_URL_MAP.get(name);
        List<Double> frequency = null;

        int timeIndex = 0;
        if (frequencyMap.containsKey(urlName)) {
            frequency = frequencyMap.get(urlName);
        } else {
            frequency = new ArrayList<Double>();
        }

        // Update the numbers for a specific date
        if (timeIndexMap.containsKey(date)) {
            timeIndex = timeIndexMap.get(date);
            f += frequency.get(timeIndex);
            frequency.add(timeIndex, f);
        } else {
            timeIndex = frequency.size();
            frequency.add(f);
        }
        
        // Check timeIndex;
        timeIndexMap.put(date, timeIndex);
        frequencyMap.put(urlName, frequency);
    }
    
    
    public double[] getFrequency(String urlName, String fromDate, String toDate) {
        List<Double> valueList = frequencyMap.get(urlName);
        int startIndex = 0;
        int endIndex = valueList.size()-1;
        
        if (fromDate!=null && timeIndexMap.containsKey(fromDate))
            startIndex = timeIndexMap.get(fromDate);
        
        if (toDate!=null && timeIndexMap.containsKey(toDate))
            endIndex = timeIndexMap.get(toDate);
        
        int rangeLength = endIndex - startIndex;
        double[] values = new double[rangeLength];
        
        
        for (int index=startIndex; index<=endIndex; index++) {
            values[index] = valueList.get(index);
        }        
        return values;
    }
    
    public double[] getFrequency(String urlName) {
        return getFrequency(urlName, null, null);
    }
    
    public double getMentionFrequency(String urlName, String dirName) {
        List<Double> frequency = frequencyMap.get(urlName);
        int timeIndex = timeIndexMap.get(getDate(dirName));
        return frequency.get(timeIndex);
    }
    
    public void reset() {        
        timeIndexMap.clear();
        Iterator<List<Double>> it = frequencyMap.values().iterator();       
        while(it.hasNext()) {
            it.next().clear();
        }
        frequencyMap.clear();
        
    }
}
