package edu.uci.ics.asterix.external.library.utils;

import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class StringUtil {
    public static final String SPECIAL_CHARACTERS = "`~!#$%^*()_+[\\];',./{}|:\"<>?";

    public static final char REPEATING_SPACES[] = { '\n', '\r', '\t', ' ' };

    public static final Set<Character> SPECIAL_CHAR_SET = toCharSet(SPECIAL_CHARACTERS);

    public static final Set<Character> REPEATING_SPACE_SET = toCharSet(REPEATING_SPACES);


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
                    if (next_c=='s') { 
                        i++;
                    } else if (next_c=='t') { // keep 't forms for now
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
                    if (count>0 && REPEATING_SPACE_SET.contains(dstStrBuffer[count - 1]))
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
        int length = str.length();
        char buffer[] = new char[length];
        
        str.getChars(0, length, buffer, 0);
        byte b[] = new byte[length];
        for (int j = 0; j < length; j++) {
            b[j] = (byte) buffer[j];
        }
        return b;
    }
    
    public static String concatenate(String strings[], char connector) {
        int i=0;
        StringBuilder sb = new StringBuilder(strings[i]);
        
        while(++i<strings.length) {
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
        int len = str.length();
        List<String> subStrings = new ArrayList<String>();

        if (len <= maxLen)
            return new String[] { str };

        int beginIndex = 0;
        int endIndex = maxLen - 1;
        while (endIndex < len) {
            endIndex = Math.min(lastIndexOf(str, beginIndex, endIndex, ' '), len - 1);
            subStrings.add(str.substring(beginIndex, endIndex));
            beginIndex = endIndex + 1;
            endIndex = beginIndex + maxLen - 2;

        }

        subStrings.add(str.substring(beginIndex, len));

        return subStrings.toArray(new String[subStrings.size()]);
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

}