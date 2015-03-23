package edu.uci.ics.asterix.external.library;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import edu.uci.ics.asterix.external.library.java.JObjects.JRecord;
import edu.uci.ics.asterix.external.library.udf.featuregeneration.RelatedEntityFeature;

public class RelatedEntityFeatureExtractionFunction extends AbstractFeatureExtractionFunction {

    @Override
    public void initialize(IFunctionHelper functionHelper) throws Exception {
        // Get the field positions
        initializeFieldPositions((JRecord) functionHelper.getArgument(0));

        // Initialize the topic entities.
        Map<String, Set<String>> topicEntities = new HashMap<>();
        KBATopicEntityLoader.loadTopicEntities(topicEntities);

        // Initialize the entity searcher (loading the entity inverted index)
        initializeSearcher();

        this.docFeature = new RelatedEntityFeature(this.searcher, topicEntities);
    }

}
