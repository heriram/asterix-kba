package edu.uci.ics.asterix.external.library.utils;

import java.lang.ref.WeakReference;
import java.util.*;

public class StringPool {
    
    public static final StringPool INSTANCE = new StringPool();
    private StringPool() {
        
    }

    private final WeakHashMap<String, WeakReference<String>> s_manualCache = 
            new WeakHashMap<String, WeakReference<String>>(100000);

    public String manualIntern(final String str) {
        final WeakReference<String> cached = s_manualCache.get(str);
        if (cached != null) {
            final String value = cached.get();
            if (value != null)
                return value;
        }
        s_manualCache.put(str, new WeakReference<String>(str));
        return str;
    }
    
}