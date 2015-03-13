package edu.uci.ics.asterix.external.library.udf.featuregeneration;

import java.util.Set;

public interface IPhraseSearcher {
    public boolean containMention(String text);

    /**
     * @param text
     *            The {@link String} of input text
     * @return
     *         Array of integer containing the list of occurrences. Returns <b>null</b> if no mention was found.
     */
    public int[] search(String text);

    public Set<String> getMentions();
}
