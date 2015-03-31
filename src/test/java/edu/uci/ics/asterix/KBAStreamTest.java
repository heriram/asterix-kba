package edu.uci.ics.asterix;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.external.udl.adapter.factory.KBAStreamFeeder;


public class KBAStreamTest {
    private static final Logger LOGGER = Logger.getLogger(KBAStreamTest.class.getName());
    private static PrintWriter out;

    private static class KBAStreamServer {
        private final DataProvider dataProvider;
        private final ExecutorService executorService;

        public KBAStreamServer(Map<String, String> configuration, int partition/*, ARecordType outputtype*/,
                OutputStream os, ExecutorService executorService) throws Exception {
            dataProvider = new DataProvider(configuration/*, outputtype*/, partition, os);
            this.executorService = executorService;
        }

        public void stop() throws IOException {
            dataProvider.stop();
        }

        public void start() {
            executorService.execute(dataProvider);
        }

    }

    public static class DataConsumer implements Runnable {
        private InputStream inputStream = null;

        public DataConsumer(InputStream inputStream) {
            this.inputStream = inputStream;
        }

        @Override
        public void run() {
            try {

                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                String temp = null;
                while ((temp = bufferedReader.readLine()) != null) {
                    out.println(temp);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static class DataProvider implements Runnable {

        public static final String KEY_BATCHSIZE = KBAStreamFeeder.KEY_BATCHIZE;

        private KBAStreamFeeder kbaDataGenerator;
        private boolean continuePush = true;
        private int batchSize = 5000;;
        private final OutputStream os;

        public DataProvider(Map<String, String> configuration /*, ARecordType outputtype*/, int partition,
                OutputStream os) throws Exception {
            batchSize = Integer.parseInt(configuration.get(KEY_BATCHSIZE));
            this.kbaDataGenerator = new KBAStreamFeeder(configuration, partition, os);
            this.os = os;
        }

        @Override
        public void run() {
            boolean moreData = true;

            while (true) {
                try {
                    while (moreData && continuePush) {
                        moreData = kbaDataGenerator.setNextRecordBatch(batchSize);
                    }
                    os.close();
                    break;
                } catch (Exception e) {
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Exception in adaptor " + e.getMessage());
                        e.printStackTrace();
                    }
                }
            }
        }

        public void stop() {
            continuePush = false;
        }

    }

    public static void main(String[] args) throws Exception {
        int partition = 0;
        long past = System.currentTimeMillis();

        out = new PrintWriter("/Users/heri/git/asterixdb/AsterixDB-KBA/asterix-adpter-test/test_output.txt");

        PipedOutputStream outputStream = new PipedOutputStream();

        PipedInputStream inputStream = new PipedInputStream(outputStream);

        Map<String, String> configuration = new HashMap<String, String>();

        configuration.put("batchsize", "20000");

        ExecutorService executorService = Executors.newCachedThreadPool();
        KBAStreamServer kbaStreamServer = new KBAStreamServer(configuration, partition, outputStream, executorService);
        DataConsumer dataConsumer = new DataConsumer(inputStream);
        Thread consumerThread = new Thread(dataConsumer);

        kbaStreamServer.start();
        consumerThread.start();

        long now = System.currentTimeMillis();
        System.out.println("Time taken: " + (now - past) + " ms");

    }

}
