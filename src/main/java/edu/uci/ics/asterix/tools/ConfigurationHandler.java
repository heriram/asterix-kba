package edu.uci.ics.asterix.tools;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by heri on 6/12/15.
 */
public class ConfigurationHandler {
    public static ConfigurationHandler INSTANCE = new ConfigurationHandler();
    private FileInputStream input;


    public static ConfigurationHandler getInstance() {
        return INSTANCE;
    }

    private Properties prop = new Properties();

    private ConfigurationHandler() {
        try {
            String workingDir = System.getProperty("user.dir");
            InputStream input = new FileInputStream(workingDir + "/config.properties");
            // load a properties file
            prop.load(input);

            prop.setProperty("working-dir", workingDir);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public String getProp(String propName) {
        return prop.getProperty(propName);
    }
}
