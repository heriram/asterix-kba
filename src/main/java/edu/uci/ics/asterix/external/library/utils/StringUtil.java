package edu.uci.ics.asterix.external.library.utils;

import java.nio.CharBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StringUtil {
    public static final String SPECIAL_CHARACTERS = "`~!#$%^*()_+[\\];'/{}|:\"<>?"; // Keeping . and ,

    public static final char REPEATING_SPACES[] = { '\n', '\r', '\t', ' ' };

    public static final Set<Character> SPECIAL_CHAR_SET = toCharSet(SPECIAL_CHARACTERS);

    public static final Set<Character> REPEATING_SPACE_SET = toCharSet(REPEATING_SPACES);

    public static final String EMPTY_STRING = "";

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

    public static String cleanText(String s) {
        int len = s.length();
        char dstStrBuffer[] = new char[len];
        char srcStrBuffer[] = s.toCharArray();
        int count = 0;
        for (int i = 0; i < len; i++) {
            char ch = s.charAt(i);
            switch (ch) {
                case '\'': // Remove "'s"
                    if (i >= (len - 1)) {
                        break;
                    }

                    char next_c = Character.toLowerCase(srcStrBuffer[i + 1]);
                    if (next_c == 's') {
                        i++;
                    } else if (next_c == 't') { // keep 't forms for now
                        dstStrBuffer[count] = ch;
                        dstStrBuffer[++count] = next_c;
                        count++;
                        i++;
                    }
                    break;

                case '.':
                case ',':
                case '?':
                    if (i >= (len - 1)) {
                        break;
                    }
                    next_c = srcStrBuffer[i + 1];
                    if (!Character.isLetterOrDigit(next_c)) {
                        break;
                    }
                case '-':
                    if (count > 0 && dstStrBuffer[count - 1] == '-') {
                        break;
                    }
                case '@':

                    if (count > 0 && dstStrBuffer[count - 1] == '@') {
                        break;
                    }
                case '_':
                    if (count > 0 && dstStrBuffer[count - 1] == '_') {
                        break;
                    }

                case '\n':
                    if (count > 0 && dstStrBuffer[count - 1] == '\n') {
                        break;
                    }
                case '\r':
                    if (count > 0 && dstStrBuffer[count - 1] == '\r') {
                        break;
                    }
                case '\t':
                    if (count > 0 && dstStrBuffer[count - 1] == '\t') {
                        break;
                    }
                case ' ':
                    if (count > 0 && dstStrBuffer[count - 1] == ' ') {
                        break;
                    }

                    dstStrBuffer[count] = ch;
                    count++;
                    break;
                default:
                    if (Character.isLetterOrDigit(ch)) {
                        dstStrBuffer[count] = Character.toLowerCase(ch);
                        count++;
                    }
            }

        }

        return (new String(dstStrBuffer, 0, count)).trim();
    }

    public static String removeSpecialChars(String s) {
        int len = s.length();
        char dstStrBuffer[] = new char[len];
        char srcStrBuffer[] = s.toCharArray();

        int count = 0;
        for (int i = 0; i < len; i++) {
            char c = srcStrBuffer[i];
            char next_c = 0;
            switch (c) {
                case '\'': // Remove "'s"
                    if (i < (len - 1)) {
                        next_c = srcStrBuffer[i + 1];
                        if (next_c == 's') {
                            i++;
                        } else if (next_c == 't') { // keep 't forms for now
                            dstStrBuffer[count] = c;
                            dstStrBuffer[++count] = next_c;
                            count++;
                            i++;
                        }
                    }
                    break;
                case '\n':
                case '\r':
                case '\t':
                case ' ':
                    if (count > 0 && REPEATING_SPACE_SET.contains(dstStrBuffer[count - 1])) {
                        break;
                    }
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
        if (str == null || str.isEmpty()) {
            return 0;
        }

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
        return concatenate(strings, 0, strings.length, connector);
    }

    public static String concatenate(String strings[], int count, char connector) {
        return concatenate(strings, 0, count, connector);
    }

    public static String concatenate(String strings[], int offset, int count, char connector) {
        if (strings == null || strings.length == 0) {
            return null;
        }

        if (strings.length == 1) {
            return strings[0];
        }

        StringBuilder sb = new StringBuilder(strings[offset]);
        int endIndex = Math.min(count, strings.length);
        for (int i = offset + 1; i < endIndex; i++) {
            sb.append(connector);
            sb.append(strings[i]);
        }

        return sb.toString();
    }

    public static String[] wrapString(String str, final int maxLen) {
        int len = str.length();
        int arrayLen = 1 + len / maxLen;

        if (len < maxLen) {
            return new String[] { str };
        }

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
            if (c == character) {
                return index;
            }
        }

        // character was not found
        return endIndex;

    }

    public static int lastIndexOf(String str, int endIndex, char character) {
        return lastIndexOf(str, 0, endIndex, character);
    }

    public static class SplittedText {
        String[] splits;
        int[] tokenOffsets;
        int[] splitLengths;

        public SplittedText(int capacity) {
            splits = new String[capacity];
            tokenOffsets = new int[capacity];
            splitLengths = new int[capacity];
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("[");
            for (int count = 0; count < splits.length; count++) {
                sb.append(tokenOffsets[count] + "::");
                sb.append(splits[count]);
                sb.append("|" + splitLengths[count]);
                if (count < splits.length - 1)
                    sb.append(", ");
            }
            sb.append(']');

            return sb.toString();
        }
    }

    public static String toString(StringBuilder sb, int length) {
        char buffer[] = new char[length];
        sb.getChars(0, length, buffer, 0);
        return new String(buffer);
    }

    /**
     * Wraping pre-tokenized text into an array of strings of length
     * 
     * <pre>
     * maxLen
     * </pre>
     * 
     * @param tokens
     * @param maxLen
     * @return
     */
    public static SplittedText splitText(String tokens[], int maxLen) {

        int tempLength = (tokens.length / 2) + 2;
        String tempBuffer[] = new String[tempLength];
        int tempOffsets[] = new int[tempLength];
        int splitLength[] = new int[tempLength];
        int count = 0;

        StringBuilder sb = new StringBuilder();
        tempOffsets[0] = 0;
        int i = 0;
        for (; i < tokens.length; i++) {
            int len = tokens[i].length();
            if ((sb.length() + len) <= maxLen) {
                sb.append(tokens[i] + " ");
            } else {
                tempBuffer[count] = toString(sb, sb.length() - 1);
                splitLength[count] = i / (count + 1);
                count++;
                sb.setLength(0);
                sb.append(tokens[i] + " ");
                tempOffsets[count] = i;

            }
        }

        SplittedText st = new SplittedText(count + 1);
        System.arraycopy(splitLength, 0, st.splitLengths, 0, count);
        st.splitLengths[count] = i / (count + 2);

        System.arraycopy(tempOffsets, 0, st.tokenOffsets, 0, count);
        st.tokenOffsets[count] = i;

        System.arraycopy(tempBuffer, 0, st.splits, 0, count);
        st.splits[count] = toString(sb, sb.length() - 1);

        return st;
    }

    /**
     * Wraping pre-tokenized text into an array of strings of length
     * 
     * <pre>
     * maxLen
     * </pre>
     * 
     * @param tokens
     * @param maxLen
     * @return
     */
    public static String[] breakString(String tokens[], int maxLen) {
        int tempLength = (tokens.length / 2) + 2;
        String tempBuffer[] = new String[tempLength];
        int count = 0;

        StringBuilder sb = new StringBuilder();
        for (String token : tokens) {
            int len = token.length();
            if ((sb.length() + len) <= maxLen) {
                sb.append(token + " ");
            } else {
                tempBuffer[count] = sb.toString();
                count++;
                sb.setLength(0);
                sb.append(token + " ");
            }
        }

        String result[] = new String[count + 1];
        System.arraycopy(tempBuffer, 0, result, 0, count);

        result[count] = sb.toString();

        return result;
    }

    /**
     * Wraping a string into an array of strings of length
     * 
     * <pre>
     * maxLen
     * </pre>
     * 
     * @param str
     * @param maxLen
     * @return
     */
    public static String[] breakString(String str, final int maxLen) {

        String subStrings[];

        int len = str.length();

        if (len <= maxLen) {
            return new String[] { str };
        }

        String tempBuffer[];

        int tempLength = (str.length() / 2) + 2;
        tempBuffer = new String[tempLength];
        int count = 0;
        int beginIndex = 0;
        int endIndex = maxLen - 1;
        while (endIndex < len) {
            endIndex = Math.min(lastIndexOf(str, beginIndex, endIndex, ' '), len - 1);
            tempBuffer[count] = str.substring(beginIndex, endIndex);
            beginIndex = endIndex + 1;
            endIndex = beginIndex + maxLen - 2;
            count++;
        }
        tempBuffer[count] = str.substring(beginIndex, len);
        count++;

        subStrings = new String[count];
        System.arraycopy(tempBuffer, 0, subStrings, 0, count);

        return subStrings;
    }

    public static void breakStringToList(List<String> resultList, String str, final int maxLen) {
        int len = str.length();

        if (len <= maxLen) {
            resultList.add(str);
        } else {
            int beginIndex = 0;
            int endIndex = maxLen - 1;
            while (endIndex < len) {
                endIndex = Math.min(lastIndexOf(str, beginIndex, endIndex, ' '), len - 1);
                resultList.add(str.substring(beginIndex, endIndex));
                beginIndex = endIndex + 1;
                endIndex = beginIndex + maxLen - 2;
            }
            resultList.add(str.substring(beginIndex, len));
        }
    }

    public static String getNormalizedString(String originalString) {
        int len = originalString.length();
        char asciiBuff[] = new char[len];
        int j = 0;
        for (int i = 0; i < len; i++) {
            char c = originalString.charAt(i);
            if (c == '\n' || c == '\t' || c == '\r') {
                asciiBuff[j] = ' ';
                j++;
            } else if (c > 0 && c <= 0x7f) {
                asciiBuff[j] = c;
                j++;
            }
        }

        return new String(asciiBuff).trim();
    }

    public static String[] tokenize(String text, String delimiters) {
        Set<Character> del = new HashSet<>();
        for (char c: delimiters.toCharArray())
            del.add(c);
        
        int len = text.length();
        String[] temp = new String[len];
        int wordCount = 0;

        char wordBuff[] = new char[temp.length];
        int index = 0;

        for (int i = 0; i < len; i++) {
            char c = text.charAt(i);
            if (del.contains(c)) {
                if (index > 0) {
                    String word = new String(wordBuff, 0, index);
                    index = 0;
                    temp[wordCount] = word;
                    wordCount++;

                }
            } else {
                wordBuff[index] = c;
                index++;
            }

        }
        
        temp[wordCount] = new String(wordBuff, 0, index).intern();
        wordCount++;
        String result[] = new String[wordCount];
        System.arraycopy(temp, 0, result, 0, wordCount);
        
        del = null;
        
        return result;
    }

    
    public static String[] tokenize(String text, char delimiter) {
        int len = text.length();
        String[] temp = new String[len];
        int wordCount = 0;

        char wordBuff[] = new char[temp.length];
        int index = 0;

        for (int i = 0; i < len; i++) {
            char c = text.charAt(i);
            if (c == delimiter) {
                if (index > 0) {
                    String word = new String(wordBuff, 0, index);
                    index = 0;
                    temp[wordCount] = word;
                    wordCount++;

                }
            } else {
                wordBuff[index] = c;
                index++;
            }

        }

        temp[wordCount] = new String(wordBuff, 0, index).intern();
        wordCount++;
        String result[] = new String[wordCount];
        System.arraycopy(temp, 0, result, 0, wordCount);

        return result;
    }

}