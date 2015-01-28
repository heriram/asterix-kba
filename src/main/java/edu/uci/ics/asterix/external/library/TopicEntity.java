package edu.uci.ics.asterix.external.library;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;

import edu.uci.ics.asterix.external.library.utils.TextAnalysis;

/**
 * Represents an input topic entity with id, name, and name variants. Related
 * entities and the textual representation associated with the entity (e.g., its
 * Wikipedia page) can be stored here, too.
 * 
 * NOTE: Entity name variants and related entities are analyzed using the
 * EntityAnalyzer (TextAnalysis.getAnalyzer()); text is analyzed using the
 * StandardAnalyzer (TextAnalysis.getStandardAnalyzer()).
 * 
 * @author Krisztian Balog <krisztian.balog@idi.ntnu.no>
 */
public class TopicEntity {

	private String urlname;

	private Set<String> nameVariants;

	private Set<String> nameVariantsAnalyzed;

	private Set<String> relatedEntities;

	private Set<String> relatedEntitiesAnalyzed;

	private Analyzer analyzer;

	private String text;

	private String[] textAnalyzed;

	public TopicEntity(String urlname) {
		analyzer = TextAnalysis.getAnalyzer();
		this.urlname = urlname;
		nameVariants = new HashSet<String>();
		nameVariantsAnalyzed = new HashSet<String>();
		// add the name itself to the variants
		// (it's important to set analyzer before this is called)
		addNameVariant(urlname);

		relatedEntities = new HashSet<String>();
		relatedEntitiesAnalyzed = new HashSet<String>();
		text = null;
	}

	public String getName() {
		return urlname;
	}

	public String[] getNameVariants() {
		return getNameVariants(false);
	}
	
	public String[] getNameVariants(boolean lowered_case) {
		if (!lowered_case)
			return (String[]) nameVariants.toArray(new String[nameVariants.size()]);
		
		Iterator<String> iterator = nameVariants.iterator();
		Set<String> names = new HashSet<String>();
		while (iterator.hasNext()) {
			names.add(iterator.next().toLowerCase().trim());
		}
		return (String[]) names.toArray(new String[names.size()]);
	}

	public List<String[]> getNameVariantsAnalyzedAsList() {
		List<String[]> nva = new ArrayList<String[]>();
		for (String nv : nameVariantsAnalyzed) {
			nva.add(nv.split(" "));
		}
		return nva;
	}

	public Set<String> getNameVariantsAnalyzed() {
		return nameVariantsAnalyzed;
	}
	
	public void addNameVariant(String name) {
		// deal with %XX
		if (name.contains("%")) {
			try {
				name = java.net.URLDecoder.decode(name, "UTF-8");
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		}

		// the entity's WP page title is also used as a name variant
		// we have to clean that
		// Remove text between "(..)" and replace "_" with " "
		name = name.replaceAll("\\(.*\\)", "").replace("_", " ");

		// name variants are stored in a hashset, so we won't have duplicates
		nameVariants.add(name);

		// create analyzed version too
		// we store these as " " separated strings (not as String[]) so that
		// it's easier to deal with duplicates
		nameVariantsAnalyzed.add(TextAnalysis.joinText(TextAnalysis.analyze(analyzer, name), ' '));
	}

	/**
	 * Add name variants from a text file (one name variant per line)
	 * 
	 * @param filename
	 */
	public void addNameVariantsFromFile(String filename) {
		try {
			BufferedReader input = new BufferedReader(new FileReader(filename));
			String line = null;
			while ((line = input.readLine()) != null) {
				if (line.length() > 0)
					addNameVariant(line);
			}
			input.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void addRelatedEntity(String name) {
		// clean name
		try {
			name = java.net.URLDecoder.decode(name, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		// Remove text between "(..)" and replace "_" with " "
		name = name.replaceAll("\\(.*\\)", "").replace("_", " ");

		// name variants are stored in a hashset, so we won't have duplicates
		relatedEntities.add(name);

		// create analyzed version too
		// we store these as " " separated strings (not as String[]) so that
		// it's easier to deal with duplicates
		relatedEntitiesAnalyzed.add(TextAnalysis.joinText(TextAnalysis.analyze(analyzer, name), ' '));
	}

	/**
	 * Add related entities from a text file (one entity per line)
	 * 
	 * @param filename
	 */
	public void addRelatedEntitiesFromFile(String filename) {
		try {
			BufferedReader input = new BufferedReader(new FileReader(filename));
			String line = null;
			while ((line = input.readLine()) != null) {
				if (line.length() > 0)
					addRelatedEntity(line);
			}
			input.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public Set<String> getRelatedEntities() {
		return relatedEntities;
	}

	public List<String[]> getRelatedEntitiesAnalyzed() {
		List<String[]> rea = new ArrayList<String[]>();
		for (String nv : relatedEntitiesAnalyzed) {
			rea.add(nv.split(" "));
		}
		return rea;
	}

	public String getText() {
		return text;
	}

	public String[] getTextAnalyzed() {
		return textAnalyzed;
	}

	public void setText(String text) {
		this.text = text;
		this.textAnalyzed = TextAnalysis.analyze(
				TextAnalysis.getStandardAnalyzer(), text);
	}

	/**
	 * Add text from a text file
	 * 
	 * @param filename
	 */
	public void addTextFromFile(String filename) {
		String text = "";
		try {
			BufferedReader input = new BufferedReader(new FileReader(filename));
			String line = null;
			while ((line = input.readLine()) != null) {
				if (line.length() > 0)
					text = text.concat(line + "\n");
			}
			input.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		setText(text);
	}

	@Override
	public String toString() {
		String str = "'" + urlname + "'\n" + "Name variants:\n";
		for (String nv : nameVariants) {
			str += "  - '" + nv + "'\n";
		}
		str += "Analyzed name variants:\n";
		for (String nva : nameVariantsAnalyzed) {
			str += "  - '" + nva + "'\n";
		}
		return str;
	}

	public int getNameVariantsCount() {
		return nameVariants.size();
	}

	public Analyzer getAnalyzer() {
		return analyzer;
	}
}
