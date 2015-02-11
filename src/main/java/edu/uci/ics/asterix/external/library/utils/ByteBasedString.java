package edu.uci.ics.asterix.external.library.utils;

import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class ByteBasedString implements CharSequence {
    public static final String SPECIAL_CHARACTERS = "`~!#$%^*()_+[\\];',./{}|:\"<>?";
    
    private byte[] value; 
    
    

    public static String cleanUp(String s) {
        final String specialChars2 = "[" + Pattern.quote(SPECIAL_CHARACTERS) + "]+";

        s = s.replaceAll("(\\s)+", " ");
        s = s.replaceAll("'s", "");
        s = s.replaceAll(specialChars2, " ");
        return s;
    }
    
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
            subStrings.add(str.substring(beginIndex, endIndex) + '\n');
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

    @Override
    public int length() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public char charAt(int index) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        // TODO Auto-generated method stub
        return null;
    }

}