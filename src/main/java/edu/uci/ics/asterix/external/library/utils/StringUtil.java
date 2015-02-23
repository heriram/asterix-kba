package edu.uci.ics.asterix.external.library.utils;

import java.io.IOException;
import java.io.StringReader;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.base.IAObject;

public class StringUtil {
    public static final String SPECIAL_CHARACTERS = "`~!#$%^*()_+[\\];'/{}|:\"<>?"; // Keeping . and ,

    public static final char REPEATING_SPACES[] = { '\n', '\r', '\t', ' ' };

    public static final Set<Character> SPECIAL_CHAR_SET = toCharSet(SPECIAL_CHARACTERS);

    public static final Set<Character> REPEATING_SPACE_SET = toCharSet(REPEATING_SPACES);
    
    public static final String EMPTY_STRING="";

    public static Set<Character> toCharSet(char charArray[]) {
        Set<Character> charSet = new HashSet<Character>();

        for (char c : charArray) {
            charSet.add(c);
        }

        return charSet;
    }

    public static Set<Character> toCharSet(String str) {
        char chars[] = str.toCharArray();
        return toCharSet(chars);
    }

    public static String removeSpecialChars(String s) {
        int len = s.length();
        char dstStrBuffer[] = new char[len];
        char srcStrBuffer[] = s.toCharArray();

        int count = 0;
        for (int i = 0; i < len; i++) {
            char c = srcStrBuffer[i];
            switch (c) {
                case '\'': // Remove "'s"
                    char next_c = srcStrBuffer[i + 1];
                    if (next_c == 's') {
                        i++;
                    } else if (next_c == 't') { // keep 't forms for now
                        dstStrBuffer[count] = c;
                        dstStrBuffer[++count] = next_c;
                        count++;
                        i++;
                    }
                    break;
                case '\n':
                case '\r':
                case '\t':
                case ' ':
                    if (count > 0 && REPEATING_SPACE_SET.contains(dstStrBuffer[count - 1]))
                        break;
                default:
                    if (!SPECIAL_CHAR_SET.contains(c)) {
                        dstStrBuffer[count] = c;
                        count++;
                    }
            }

        }

        return new String(dstStrBuffer, 0, count);
    }

    /**
     * Get the string bytes encoding
     * 
     * @param str
     * @return
     */
    public static byte[] getBytes(String str) {
        /*int length = str.length();
        char buffer[] = new char[length];
        
        str.getChars(0, length, buffer, 0);
        byte b[] = new byte[length];
        for (int j = 0; j < length; j++) {
            b[j] = (byte) buffer[j];
        }
        return b;*/

        char[] buffer = str.toCharArray();
        byte[] b = new byte[buffer.length << 1];
        for (int i = 0; i < buffer.length; i++) {
            int bpos = i << 1;
            b[bpos] = (byte) ((buffer[i] & 0xFF00) >> 8);
            b[bpos + 1] = (byte) (buffer[i] & 0x00FF);
        }
        return b;
    }

    public static int sizeOfString(String str) {
        if (str == null || str.isEmpty())
            return 0;
        
        int size = 0;
        int strlen = str.length();
        char c;
        int i = 0;

        for (; i < strlen; i++) {
            c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                size++;
            } else if (c > 0x07FF) {
                size += 3;
            } else {
                size += 2;
            }
        }
        return size;
    }

    public static String concatenate(String strings[], char connector) {
        int i = 0;
        StringBuilder sb = new StringBuilder(strings[i]);
        while (++i < strings.length) {
            sb.append(connector);
            sb.append(strings[i]);
        }

        return sb.toString();
    }

    public static String[] wrapString(String str, final int maxLen) {
        int len = str.length();
        int arrayLen = 1 + (int) len / maxLen;

        if (len < maxLen)
            return new String[] { str };

        String strArray[] = new String[arrayLen];
        CharBuffer strBuff = CharBuffer.wrap(str);

        int remaining = 0;
        char b[] = null;
        int i = 0;
        while ((remaining = strBuff.remaining()) > maxLen) {
            b = new char[maxLen];
            strBuff.get(b);
            strArray[i] = new String(b);
            i++;
        }
        b = new char[remaining];
        strBuff.get(b);
        strArray[i] = new String(b);

        return strArray;

    }

    public static int lastIndexOf(String str, int beginIndex, int endIndex, char character) {
        final char[] strChars = str.toCharArray();
        int index = endIndex;
        int fromIndex = beginIndex >= 0 ? beginIndex : 0;
        for (; index > fromIndex; index--) {
            char c = strChars[index];
            if (c == character)
                return index;
        }

        // character was not found
        return endIndex;

    }

    public static int lastIndexOf(String str, int endIndex, char character) {
        return lastIndexOf(str, 0, endIndex, character);
    }

    public static String[] breakString(String str, final int maxLen) {
        List<String> subStrings = breakStringToList(str, maxLen);
        return subStrings.toArray(new String[subStrings.size()]);
    }

    public static List<String> breakStringToList(String str, final int maxLen) {
        int len = str.length();
        List<String> subStrings = new ArrayList<String>();

        if (len <= maxLen) {
            subStrings.add(str);
        } else {
            int beginIndex = 0;
            int endIndex = maxLen - 1;
            while (endIndex < len) {
                endIndex = Math.min(lastIndexOf(str, beginIndex, endIndex, ' '), len - 1);
                subStrings.add(str.substring(beginIndex, endIndex));
                beginIndex = endIndex + 1;
                endIndex = beginIndex + maxLen - 2;
            }
            subStrings.add(str.substring(beginIndex, len));
        }

        return subStrings;
    }
    

    public static String[] tokenize(String string, char delimiter) {
        ThreadLocal<String[]> tempArray = new ThreadLocal<String[]>();
        String[] temp = tempArray.get();
        int tempLength = (string.length() / 2) + 2;

        if (temp == null || temp.length < tempLength) {
            temp = new String[tempLength];
            tempArray.set(temp);
        }

        int wordCount = 0;
        int i = 0;
        int j = string.indexOf(delimiter);

        while (j >= 0) {
            String word = string.substring(i, j).trim();
            if (!word.isEmpty())
                temp[wordCount++] = word;
            i = j + 1;
            j = string.indexOf(delimiter, i);
        }

        temp[wordCount++] = string.substring(i);

        String[] result = new String[wordCount];
        System.arraycopy(temp, 0, result, 0, wordCount);
        return result;
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
    public static void analyze(String text, Map<String, Set<Integer>> analyzed) throws Exception {
        if (analyzed == null)
            throw new Exception("Result \"Map\" was not initialized. Cannot be null.");

        if (text.trim().isEmpty())
            return;

        Set<Integer> positions = null;
        String tokens[] = tokenize(text, ' ');
        int pos = 0;
        for (String term : tokens) {
            if (analyzed.containsKey(term))
                positions = analyzed.get(term);
            else
                positions = new HashSet<Integer>();

            positions.add(pos);
            analyzed.put(term, positions);
            pos++;
        }

    }

}