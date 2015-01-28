package edu.uci.ics.asterix.external.library.utils;

import java.io.File;
import java.io.IOException;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;

/**
 * Check whether a given text is english or not.
 * 
 * @see no.ntnu.utils.LanguageDetector
 * 
 * @author Heri Ramampiaro <heri@ntnu.no>
 * @author Krisztian Balog <krisztian.balog@idi.ntnu.no>
 * 
 */

public class EnglishLanguageDetector {
	// Default name of the directory holding the language profiles
	private final String DEFAULT_LANG_PROFILE_DIR = "language_profiles";

	/**
	 * Class constructor, including initializing the language detection
	 */
	public EnglishLanguageDetector() {
		try {
			// Get the profile directory
			String profileDirectory = (new File(".")).getCanonicalPath() + "/"
					+ DEFAULT_LANG_PROFILE_DIR;

			// Initialize the language detection
			DetectorFactory.loadProfile(profileDirectory);

		} catch (LangDetectException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Checks whether a text is in English
	 * 
	 * @param text
	 * @return
	 * @throws LangDetectException
	 */
	public boolean isEnglish(String text) throws LangDetectException {
		Detector detector = DetectorFactory.create();
		detector.append(text);
		boolean isEnglish = detector.detect().equals("en");

		// Returns true if the language detected is English (en)
		return isEnglish;
	}
}
