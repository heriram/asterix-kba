package edu.uci.ics.asterix.external.library.adaptor;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;

/**
 * Detects the language for a given text. Must be initialized by calling
 * init(<language_profile_directory>) to work.
 * 
 * @author Heri Ramampiaro <heri@ntnu.no>
 * @author Krisztian Balog <krisztian.balog@idi.ntnu.no>
 * 
 */

public class LanguageDetector {

	private final String DEFAULT_LANG_PROFILE_DIR = "language_profiles";
	private static final Logger log = Logger.getLogger(LanguageDetector.class);

	public LanguageDetector() {
		try {
			// Get the current base directory
			File directory = new File(".");
			this.init(directory.getCanonicalPath() + "/"
					+ DEFAULT_LANG_PROFILE_DIR);

		} catch (LangDetectException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Initialize the language detection, including specification of the
	 * language profile directory
	 * 
	 * @param profileDirectory
	 * @throws LangDetectException
	 */

	public void init(String profileDirectory) throws LangDetectException {
		DetectorFactory.loadProfile(profileDirectory);

	}

	/**
	 * Detect the language. Return the shorten name if found
	 * 
	 * @param text
	 * @return
	 * @throws LangDetectException
	 */
	public String detect(String text) throws LangDetectException {
		Detector detector = DetectorFactory.create();
		detector.append(text);

		return detector.detect();
	}

	/**
	 * Checks whether a text is in English
	 * 
	 * @param text
	 * @return
	 * @throws LangDetectException
	 */
	public boolean isEnglish(String text) { // throws LangDetectException {
		boolean en = false;
		try {
			String lang = detect(text);
			// Returns true if the language detected is English (en)
			en = lang.equalsIgnoreCase("en");
		} catch (LangDetectException e) {
			log.error("Error generated when trying to detect language for \"" + text + "\"");
			e.printStackTrace();
		}
		return en;
	}

	public static void main(String[] args) throws LangDetectException,
			IOException {
		String text[] = {
				"Aharon Barak (born 16 September 1936) is a Professor's of Law at the Interdisciplinary\n"
						+ "Center in Herzliya and a lecturer in law at the Hebrew University of Jerusalem, the Yale Law School, Georgetown\n\n"
						+ " University Law Center, and the University of Toronto Faculty of Law."
						+ "Barak was President of the Supreme Court of Israel from 1995 to 2006. Prior to that, he served"
						+ " as a Justice on the Supreme Court of Israel (1978�95), as the Attorney General of Israel (1975�78)"
						+ ", and as the Dean of the Law Faculty of the Hebrew University of Jerusalem (1974�75)",
				// Second text
				"Thorbj�rn Egner (1912�1990) var en norsk forfatter, illustrat�r, billedkunstner, reklametegner, visedikter, komponist og "
						+ "lesebokredakt�r."
						+ "Han er kjent i flere land for sine fortellinger for barn, s�rlig de tre hovedverkene Karius og Baktus (1949), Klatremus og "
						+ "de andre dyrene i Hakkebakkeskogen (1953) og Folk og r�vere i Kardemomme by (1955), som han selv bearbeidet og framf�rte i "
						+ "flere medier: i Barnetimen i NRK, i bokform, som dukketeater, som dukkefilm og som sceneteater. Selv regnet han de 16 bindene "
						+ "av Thorbj�rn Egners leseb�ker (1950�72) som sitt hovedverk, og som det viktigste han hadde gjort. Egner var ogs� visebokredakt�r, "
						+ "stilte ut tresnitt p� H�stutstillingen og ga ut to kulturhistoriske reiseskildringer for voksne."
						+ "Forfatterskapet har to poler. P� den ene siden var Egner, slik det ble p�pekt i en biografi til 100-�rsjubileet, �en dannelsesagent,"
						+ " en nasjonal strateg som produserte den samme ideologien for den oppvoksende slekt, som Arbeiderpartiet presenterte for de voksne�, "
						+ "som fremhevet flid og samarbeid. Dette kommer s�rlig til uttrykk i Hakkebakkeskogen og Kardemomme by. P� den annen side var det "
						+ "sentralt for ham � understreke kunstens egenverdi, og nytten ved skj�nnheten og det unyttige. Dette kommer s�rlig til uttrykk i den "
						+ "bohemaktige hovedpersonen Klatremus i Hakkebakkeskogen, og i det siste verket Musikantene kommer til byen (1978)."
						+ "B�kene og platene hans solgte i store opplag, og er blitt oversatt til en rekke spr�k. Han mottok St. Olavs Orden i 1972, s�rlig "
						+ "for arbeidet med leseb�kene. Forn�yelsesparken Kardemomme by i Kristiansand Dyrepark ble �pnet i 1991, og vil i 2014 bli supplert av "
						+ "en �Hakkebakkeskog�." };

		LanguageDetector ld = new LanguageDetector();
		long time = System.currentTimeMillis();
		System.out.println("The language for text " + 1 + " is: Engish? "
				+ ld.isEnglish(text[0]));
		time = System.currentTimeMillis() - time;
		System.out.println("Detection time: " + time + "ms.");

		time = System.currentTimeMillis();
		System.out.println("\nThe language for text " + 2 + " is: Engish? "
				+ ld.isEnglish(text[1]));
		time = System.currentTimeMillis() - time;
		System.out.println("Detection time: " + time + "ms.");
	}
}
