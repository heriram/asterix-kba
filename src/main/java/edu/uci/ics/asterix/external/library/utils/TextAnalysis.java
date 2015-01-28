package edu.uci.ics.asterix.external.library.utils;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

/**
 * General text-analysis tools
 * 
 * @author Krisztian Balog <krisztian.balog@idi.ntnu.no>
 * @author Heri Ramampiaro <heri@idi.ntnu.no>
 */
public class TextAnalysis {

    public static Analyzer getAnalyzer() {
        return new EntityAnalyzer();
    }

    public static Analyzer getStandardAnalyzer() {
        return new StandardAnalyzer();
    }

    
    /**
     * Analyze text using a specific Analyzer
     * 
     * @param analyzer
     * @param text
     * @return
     */
    public static String[] analyze(Analyzer analyzer, String text) {
        List<String> analyzedText = new ArrayList<String>();

        try {
            analyze(analyzer, text, analyzedText);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return analyzedText.toArray(new String[analyzedText.size()]);
    }

    /**
     * Analyze text using a specific Analyzer.
     * Places the analysed text in an ArrayList.
     * 
     * @param analyzer
     * @param text
     * @return
     * @throws Exception
     */
    public static void analyze(Analyzer analyzer, String text, Collection<String> analyzed) throws Exception {
        if (analyzed == null)
            throw new Exception("Cannot analyse text to null");

        if (text.trim().isEmpty())
            return;

        try {
            TokenStream ts = analyzer.tokenStream(null, new StringReader(text));
            ts.reset();
            while (ts.incrementToken()) {
                analyzed.add(ts.getAttribute(CharTermAttribute.class).toString());
            }
            ts.end();
            ts.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    
    public static void analyze(String text, Collection<String> analyzed) throws Exception {
        analyze(new EntityAnalyzer(), text, analyzed);
    }

    /**
     * Analyze text using a specific Analyzer.
     * Places the analysed text in an HashMap to keep track the positions.
     * 
     * @param analyzer
     * @param text
     * @return
     * @throws Exception
     */
    public static void analyze(Analyzer analyzer, String text, Map<String, Set<Integer>> analyzed) throws Exception {
        if (analyzed == null)
            throw new Exception("Cannot analyse text to null");

        if (text.trim().isEmpty())
            return;

        Set<Integer> positions = null;
        try {
            TokenStream ts = analyzer.tokenStream(null, new StringReader(text));
            ts.reset();
            int pos = 0;
            while (ts.incrementToken()) {
                CharTermAttribute buffer = ts.getAttribute(CharTermAttribute.class);
                String term = buffer.toString();

                if (analyzed.containsKey(term))
                    positions = analyzed.get(term);
                else
                    positions = new HashSet<Integer>();

                positions.add(pos);
                analyzed.put(term, positions);
                pos++;
            }
            ts.end();
            ts.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * Analyze text using the default analyzer (StandardAnalyzer)
     * 
     * @param text
     * @return
     */
    public static String[] analyze(String text) {
        return analyze(getAnalyzer(), text);
    }

    public static String joinText(String strings[], char delim) {
        StringBuilder sb = new StringBuilder();
        
        int i=0;
        for(String string: strings) {
            sb.append(string);
            if(++i<strings.length)
                sb.append(delim);
        }
        return sb.toString();
    }
    
    public static void main(String[] args) throws Exception {
        String text = "Aharon \"Barak\" (@aharon_barak) (born 16 September 1936) is a Professor's of Law at the Interdisciplinary\n"

                + "Center in Herzliya and a lecturer in law at the Hebrew University of Jerusalem, the Yale Law School, Georgetown\n\n"
                + " University Law Center (g&b), and the University of Toronto Faculty of Law."
                + "Barak was President of the Supreme Court of Israel from 1995 to 2006. Prior to that, he served"
                + " as a Justice on the Supreme Court of Israel (1978�95), as the Attorney General of Israel (1975�78)"
                + ", and as the Dean of the Law Faculty of the Hebrew University of Jerusalem (1974�75)";
        String text2 = "I QUIT!  Yes, you read that right; I quit, well, resigned from my teaching job of eight years. I "
                + "loved the children and had always wanted to be a teacher, but was unexpectedly offered an impossible-to-refuse "
                + "full time position in affiliate marketing ??? an industry quite different from my previous career."
                +

                "While teaching, I did affiliate marketing part time for over six years, so I have a strong enough background to "
                + "enable me to enter the industry full time with confidence. Conversely, it is a bit intimidating to suddenly give up "
                + "a job you are good at and have done for so long, for a career in something you???ve done part-time in a still fairly new industry. "
                + " I went from co-managing one affiliate program to suddenly managing four programs independently. I was very excited for my new "
                + "responsibilities because I love to work and interact with affiliates, but with this huge change and the additional three programs "
                + "came feelings of being overwhelmed at times. So, what did I do? The teacher in me took over and I educated myself. "
                +

                "Shortly before my career transition, Geno Prussakov released his book, Affiliate Program Management: An Hour A Day. I had "
                + "a feeling this book could help me not have to work every waking hour, and it did. I approached Affiliate Program Management: "
                + "An Hour A Day as a one-stop shop when it comes to what I need to know about affiliate program management. \n\n"
                +

                "Geno breaks it into basic elements, so that someone brand new to affiliate marketing could read, understand and obtain enough "
                + "knowledge to successfully move forward. I consider myself well-seasoned in affiliate marketing and I still learned a lot. "
                +

                "The programs I manage are already well established. However, it was enlightening to read the detailed steps the book suggests "
                + "for the development of a merchant program, so I could go back and check to be sure nothing was skipped over. "
                +

                "I am always looking for clear cut methods, tips, and tricks, to improve myself in my work and I learned many about affiliate "
                + "program management techniques in this book that I haven???t read elsewhere. "
                +

                "One of my favorite tips is the suggestion to occasionally get away from the computer and pick up the phone or send a piece of "
                + "???regular??? mail. Affiliates like to feel as though you are taking time and genuinely show interest in helping and working with them. "
                +

                "The way in which Geno literally outlines affiliate program management in his book is easy to follow and refer back to certain points when "
                + "needed. That, combined with the abundance of knowledge about program management, this book has definitely helped me successfully transition "
                + "into managing multiple programs with confidence. It has certainly validated my choice of making the transition to full time affiliate marketing."
                +

                "Jill is Affiliate Manager of Girly Checks, Carousel Checks, Extra Value Checks, and Business Checks."
                +

                "Download the entire FeedFront issue 15 here ??? http://www.scribd.com/doc/61379014/FeedFront-Magazine-Issue-15 "
                +

                "FeedFront issue 15 articles can be found here as well: http://feedfront.com/archives/article00date/2011/09";

        System.out.println("Analyzed text (EntityAnalyzer): ");
        List<String> analyzedText = new ArrayList<String>();
        long s = System.currentTimeMillis();
        TextAnalysis.analyze(new EntityAnalyzer(), text, analyzedText);
        long e = System.currentTimeMillis() - s;

        System.out.println("Time spent analyzing the text: " + e + " ms.");
        System.out.println("'" + joinText(analyzedText.toArray(new String[analyzedText.size()]), ' ') + "'");

        Map<String, Set<Integer>> analyzedTextMap = new HashMap<String, Set<Integer>>();
        s = System.currentTimeMillis();
        TextAnalysis.analyze(new EntityAnalyzer(), text, analyzedTextMap);
        e = System.currentTimeMillis() - s;
        System.out.println("Time spent analyzing (map) the text: " + e + " ms.");

        Iterator<Entry<String, Set<Integer>>> it = analyzedTextMap.entrySet().iterator();

        while (it.hasNext()) {
            Entry<String, Set<Integer>> entry = it.next();
            System.out.print(entry.getKey());
            Set<Integer> valset = entry.getValue();
            System.out.println(" -- Positions: " + valset);

        }

        System.out.println();
        String[] analyzedText2 = TextAnalysis.analyze("Basic Element's. first appearance");
        System.out.println("'" + joinText(analyzedText2, ' ') + "'");
    }

}
