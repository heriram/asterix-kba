package edu.uci.ics.asterix.external.library.udf.utils;

import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.tools.ConfigurationHandler;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.User;
import twitter4j.conf.ConfigurationBuilder;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.external.library.KBATopicEntityLoader;
import edu.uci.ics.asterix.external.util.TwitterUtil;
import edu.uci.ics.asterix.external.util.TwitterUtil.AuthenticationConstants;

public class WikipediaPageDownloader {
    private static final Logger LOGGER = Logger.getLogger(WikipediaPageDownloader.class.getName());
    private final String topicFile = KBATopicEntityLoader.ENTITY_LOOKUP_FILE;
    private List<String> entityUrlList;
    private Twitter twitter;

    public WikipediaPageDownloader() {
        entityUrlList = new ArrayList<String>();
        loadTopicUrls();
        Map<String, String> configuration = new HashMap<>();
        configuration.put(TwitterUtil.AuthenticationConstants.AUTHENTICATION_MODE,
                TwitterUtil.AuthenticationConstants.AUTHENTICATION_MODE_FILE);
        try {
            TwitterUtil.initializeConfigurationWithAuthInfo(configuration);
        } catch (AsterixException e) {
            e.printStackTrace();
        }
        twitter = TwitterUtil.getTwitterService(configuration);
    }

    public String getTwitterContent(String username) {
        StringBuilder sb = new StringBuilder();
        try {

            User user = twitter.showUser(username); // this line
            if (user.getStatus() != null) {
                sb.append("@" + user.getScreenName() + " - ");
                sb.append(user.getDescription());
             } else {
                 sb.append("@" + user.getScreenName());
            }
        } catch (TwitterException e) {
            LOGGER.log(Level.SEVERE, "TwitterException: Twitter - showUser failing...");
            e.printStackTrace();
        }
        return sb.toString();
    }

    private void loadTopicUrls() {
        try {
            InputStream is = new FileInputStream(topicFile);

            // Load to json object
            JSONObject ejson = new JSONObject(KBATopicEntityLoader.readInputStreamAsString(is));

            /* Convert json to a list of doc fields - ie., list of stream docs */
            Iterator<String> it = ejson.keys();

            while (it.hasNext()) {
                entityUrlList.add(it.next());
            }

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error while loading topic entities and name variants");
            e.printStackTrace();
        }
        LOGGER.log(Level.INFO, entityUrlList.size() + " topic URLs loaded");
    }

    public String[] getUrlList() {
        return entityUrlList.toArray(new String[entityUrlList.size()]);
    }

    public String getWebContent(String url) {
        Document doc = null;
        String content = null;
        try {
            doc = Jsoup.connect(url).get();
            content = doc.body().text();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return content;
    }

    public void saveTextContent(String fileName, String content) {
        try {
            BufferedOutputStream bufferedOutput = new BufferedOutputStream(new FileOutputStream(fileName));
            bufferedOutput.write(content.getBytes());
            bufferedOutput.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String args[]) {
        WikipediaPageDownloader wpd = new WikipediaPageDownloader();

        String urls[] = wpd.getUrlList();

        for (String url : urls) {
            String s[] = url.split("/");
            String name = s[s.length - 1].replaceAll("\\(.*\\)", "");
            if (url.contains("twitter.com"))
                wpd.saveTextContent(name +".txt", wpd.getTwitterContent(name));
            else {
                wpd.saveTextContent(name +".txt", wpd.getWebContent(url));
            }
        }
    }
}
