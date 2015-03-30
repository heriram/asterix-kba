package edu.uci.ics.asterix.external.dataset.adapter;

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.external.library.utils.KBACorpusFiles;
import edu.uci.ics.asterix.om.types.ARecordType;

public class KBADataProvider implements Runnable {
    protected static final Logger LOGGER = Logger.getLogger(FeedClient.class.getName());
    
    private KBAFileReader contentProvider;
    private boolean continuePush = true;
    private List<File> dateHourDirectoryList;
    int numFiles = 0;
    int numHours = 0;

    public KBADataProvider(File[] dateHourDirectoryList, ARecordType outputtype, int maxFrameSize, 
            BlockingQueue<Map<String, Object>> dataInputQueue, KBAFileReader chunkReader) {
        this.contentProvider = chunkReader;
        this.dateHourDirectoryList = Arrays.asList(dateHourDirectoryList);
        this.numHours = dateHourDirectoryList.length;
    }

    @Override
    public void run() {
        LOGGER.log(Level.INFO, "Feed adapter created and loaded successfully. Now start ingesting data.");
        Iterator<File> directoryIterator = dateHourDirectoryList.iterator();
        while (true) {
            try {

                while (directoryIterator.hasNext() && continuePush) {
                    File dateHourDir = directoryIterator.next(); //dateHourDirectoryList[index];

                    LOGGER.log(Level.INFO, "Reading chunks from " + dateHourDir);

                    File[] chunks = KBACorpusFiles.getXZFiles(dateHourDir);
                    String dirName = dateHourDir.getName();
                    // Get and feed next batch
                    for (File chunk : chunks) {
                        if (((++this.numFiles) % 50) == 0) {
                            LOGGER.log(Level.INFO, "Processed " + (this.numFiles) + " files out of "
                                    + (chunks.length * this.numHours) + ".");
                        }
                        if (!continuePush)
                            break;
                        
                        contentProvider.readChunk(chunk, dirName);
                    }
                }
                break;
            } catch (Exception e) {
                e.printStackTrace();
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning("Exception in adaptor " + e.getMessage());
                }
            }
        }
        LOGGER.log(Level.INFO, "Reached the end of this feed - i.e., no more data to push.");
    }

    public void stop() {
        continuePush = false;
    }

}