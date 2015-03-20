package edu.uci.ics.asterix.external.library.utils;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.Set;

public class KBACorpusFiles {

    public static String DEFAULT_CORPUS = "/Users/heri/git/asterixdb/AsterixDB-KBA/corpus";
    // Matching dir names like 2011-10-23-20
    public static final String DATE_HOUR_DIRNAME_PATTERN = "^(20\\d\\d)-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])-([01][0-9]|2[0-3])$";

    public static File[] getDateHourDirs(String corpusdir) {
        File directory = new File(corpusdir);
        if (directory.exists() && directory.isDirectory()) {
            File[] files = directory.listFiles(new FileFilter() {
                @Override
                public boolean accept(File dir) {
                    if (dir.getName().matches(DATE_HOUR_DIRNAME_PATTERN))
                        return true;
                    return false;
                }
            });

            return files;
        } else
            return null;
    }

    public static int getTotalNumberOfHourDirs(String corpusdir) {
        File directory = new File(corpusdir);
        if (!directory.exists() || !directory.isDirectory())
            return -1;

        else {
            String[] files = directory.list(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    if (name.matches(DATE_HOUR_DIRNAME_PATTERN))
                        return true;
                    return false;
                }
            });

            return files.length;
        }
    }

    public static void getDateHourDirs(Set<File> file_set, String corpusdir) {
        File directory = new File(corpusdir);
        if (directory.exists() && directory.isDirectory()) {
            File[] files = directory.listFiles(new FileFilter() {
                @Override
                public boolean accept(File dir) {
                    if (dir.getName().matches(DATE_HOUR_DIRNAME_PATTERN))
                        return true;
                    return false;
                }
            });

            file_set.addAll(Arrays.asList(files));
        }
    }

    public static void getDateHourDirs(Set<File> file_set) {
        getDateHourDirs(file_set, DEFAULT_CORPUS);
    }

    public static File[] getDateHourDirs() {
        return getDateHourDirs(DEFAULT_CORPUS);
    }

    public static String[] getDateHourDirNames(String corpusdir) {

        File directory = new File(corpusdir);
        String[] files = directory.list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                if (name.matches(DATE_HOUR_DIRNAME_PATTERN))
                    return true;
                return false;
            }
        });
        return files;
    }

    public static String[] getDateHourDirNames(String corpusdir, String fromTime) {

        File directory = new File(corpusdir);
        final DateHour from_time = new DateHour(fromTime);

        String[] files = directory.list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                if (!name.matches(DATE_HOUR_DIRNAME_PATTERN))
                    return false;
                if (from_time.compareTo(name) < 0)
                    return false;

                return true;
            }
        });
        return files;
    }

    public static String[] getDateHourDirNames(String corpusdir, String fromTime, String toTime) {

        File directory = new File(corpusdir);
        final DateHour from_time = new DateHour(fromTime);
        final DateHour to_time = new DateHour(toTime);

        String[] files = directory.list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                if (!name.matches(DATE_HOUR_DIRNAME_PATTERN))
                    return false;
                if (from_time.compareTo(name) >= 0 && to_time.compareTo(name) <= 0)
                    return true;

                return false;
            }
        });

        return files;
    }

    public static String[] getDateHourDirNames(String corpusdir, String fromTime, int duration) {
        String to_time = DateHour.computeEndDate(fromTime, duration);
        return getDateHourDirNames(corpusdir, fromTime, to_time);
    }

    public static String[] getDateHourDirNames() {
        return getDateHourDirNames(DEFAULT_CORPUS);
    }

    public static File[] getFiles(File dhd, final String ext) {
        File[] files = null;

        if (dhd.isDirectory()) {
            files = dhd.listFiles(new CorpusFileFilter(ext));
        }
        return files;
    }
    
    public static class CorpusFileFilter implements FileFilter {
        private String extension;
        
        public static final CorpusFileFilter GPG_FILTER = new CorpusFileFilter(".gpg");
        public static final CorpusFileFilter XZ_FILTER = new CorpusFileFilter(".xz");
        public static final CorpusFileFilter SC_FILTER = new CorpusFileFilter(".sc");
        public static final CorpusFileFilter ADM_FILTER = new CorpusFileFilter(".adm");
        public static final CorpusFileFilter JSON_FILTER = new CorpusFileFilter(".json");
        
        public CorpusFileFilter(String ext) {
            this.extension = ext;
        }

        @Override
        public boolean accept(File pathname) {
            String name = pathname.getName();
            if (name.startsWith("."))
                return false;
            
            if (name.endsWith(extension))
                return true;
            
            return false;
        }
        
    }
    

    public static File[] getFiles(String dh_dir, String ext) {
        return getFiles(new File(dh_dir), ext);

    }

    public static File[] getXZFiles(File dhd) {
        return getFiles(dhd, ".xz");
    }

    public static File[] getGPGFiles(File dhd) {
        return getFiles(dhd, ".gpg");
    }

    public static File[] getSCFiles(File dhd) {
        return getFiles(dhd, ".sc");
    }

    private static class DateHour implements Comparable<String> {
        private String dataHour;

        public DateHour(String date_hour) {
            dataHour = date_hour;
        }

        private static int[] durationToDate(int number_hours) {
            int year = 0;
            int month = 0;
            int day = 0;
            int hour = 0;
            int rest = 0;

            year = (int) number_hours / 8760;
            rest = number_hours % 8760;

            month = (int) rest / 730;
            if (month > 0)
                rest = rest % 730;

            day = rest / 24;

            if (day > 0)
                rest = rest % 24;

            hour = rest;

            return new int[] { year, month, day, hour };

        }

        public static String computeEndDate(String from_date, int duration) {
            String s[] = from_date.split("\\-");
            int dt[] = durationToDate(duration - 1);

            int y = Integer.parseInt(s[0]) + dt[0];
            int m = Integer.parseInt(s[1]) + dt[1];
            int d = Integer.parseInt(s[2]) + dt[2];
            int h = Integer.parseInt(s[3]) + dt[3];

            return String.format("%04d-%02d-%02d-%02d", y, m, d, h);
        }

        @Override
        public int compareTo(String o) {
            int dh1 = Integer.parseInt(dataHour.replaceAll("\\-", ""));
            int dh2 = Integer.parseInt(o.replaceAll("\\-", ""));
            return dh2 - dh1;
        }

    }

}
