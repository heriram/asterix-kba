package edu.uci.ics.asterix.external.library.statistics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.uci.ics.asterix.external.library.KBATopicEntityLoader;

public class EntityMentionCounter {
    private static Map<String, String> entityURLMap = new HashMap<String, String>(); // Mapping between url_name and an entity name variant
    static {
        KBATopicEntityLoader.buildNameURLMap(entityURLMap);
    }

    private Map<String, List<Double>> mentionFrequency; // Total number of mentions per day
    private Map<String, List<Double>> mentionDocFrequency; // Number of docs mentioning entities per day
    private Map<String, Integer> timeIndexMap; // Time index map

    public static final EntityMentionCounter INSTANCE = new EntityMentionCounter();

    private EntityMentionCounter() {
        mentionFrequency = new HashMap<>();
        mentionDocFrequency = new HashMap<>();
        timeIndexMap = new HashMap<>();
    }
    
    public void reset() {
        mentionFrequency.clear();
        mentionDocFrequency.clear();
        timeIndexMap.clear();
    }

    private String getDate(String dirName) {
        int endIndex = dirName.lastIndexOf('-');
        return dirName.substring(0, endIndex);
    }

    public void addMentionNumbers(String name, String dirName, double docMentions, double mentionNumber) {
        String date = getDate(dirName);

        double numberDocs = docMentions;
        double totalNumber = mentionNumber;

        String urlName = entityURLMap.get(name);
        List<Double> frequency = null;
        List<Double> docFrequency = null;

        int timeIndex = 0;
        if (mentionFrequency.containsKey(urlName)) {
            frequency = mentionFrequency.get(urlName);
            docFrequency = mentionDocFrequency.get(urlName);
        } else {
            frequency = new ArrayList<Double>();
            docFrequency = new ArrayList<Double>();
        }

        // Update the numbers for a specific date
        if (timeIndexMap.containsKey(date)) {
            timeIndex = timeIndexMap.get(date);
            totalNumber += frequency.get(timeIndex);
            numberDocs += docFrequency.get(timeIndex);
            frequency.add(timeIndex, totalNumber);
            docFrequency.add(timeIndex, numberDocs);
        } else {
            timeIndex = frequency.size();
            frequency.add(totalNumber);
            docFrequency.add(numberDocs);
        }

        // Check timeIndex;
        timeIndexMap.put(date, timeIndex);
        mentionFrequency.put(urlName, frequency);
        mentionDocFrequency.put(urlName, docFrequency);
    }

    public double[] getMentionDocFrequency(String urlName) {
        return getMentionDocFrequency(urlName, null, null);
    }

    public double[] getMentionDocFrequency(String urlName, String fromDate, String toDate) {
        return getFrequency(mentionDocFrequency.get(urlName), urlName, fromDate, toDate);
    }
    
    public double[] getMentionFrequency(String urlName) {
        return getMentionFrequency(urlName, null, null);
    }

    public double[] getMentionFrequency(String urlName, String fromDate, String toDate) {
        return getFrequency(mentionFrequency.get(urlName), urlName, fromDate, toDate);
    }
    
    private double[] getFrequency(List<Double> valueList, String urlName, String fromDate, String toDate) {
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

    public double getMentionFrequency(String urlName, String dirName) {
        List<Double> frequency = mentionFrequency.get(urlName);
        int timeIndex = timeIndexMap.get(getDate(dirName));
        return frequency.get(timeIndex);
    }

    public double getDocMentionFrequency(String urlName, String dirName) {
        List<Double> frequency = mentionDocFrequency.get(urlName);
        int timeIndex = timeIndexMap.get(getDate(dirName));
        return frequency.get(timeIndex);
    }
}
