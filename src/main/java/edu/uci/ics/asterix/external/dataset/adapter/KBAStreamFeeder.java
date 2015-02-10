/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.external.dataset.adapter;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.external.dataset.adapter.KBADataGenerator.KBAStreamDocIterator;
import edu.uci.ics.asterix.external.library.utils.KBAStreamDocument;


public class KBAStreamFeeder {

    private static Logger LOGGER = Logger.getLogger(KBAStreamFeeder.class.getName());

    public static final String KEY_DURATION = "duration";
    public static final String KEY_BATCHIZE = "batch-size";

    private KBAStreamDocIterator kbaStreamIterator = null;
    private int partition;
    private int streamItemCount = 0;
    private int frameStreamItemCount = 0;
    private int numFlushedstreamItem = 0;
    private OutputStream os;
    private KBADataGenerator kbaDataGenerator = null;
    private ByteBuffer outputBuffer = ByteBuffer.allocate(32 * 1024);

    public int getStreamItemCount() {
        return streamItemCount;
    }

    public KBAStreamFeeder(Map<String, String> configuration, int partition, OutputStream os) throws Exception {
        this.partition = partition;
        int batch_size = Integer.parseInt(configuration.get(KEY_BATCHIZE));
        kbaDataGenerator = new KBADataGenerator(batch_size);
        kbaStreamIterator = kbaDataGenerator.getIterator();  //.new KBAStreamDocIterator();
        this.os = os;
    }
    
    
    private void flushBufferFrames(byte[] string_byte) throws IOException {
        int max_length = outputBuffer.limit();
        
        ByteBuffer tempBuffer = ByteBuffer.wrap(string_byte);
        int remaining = tempBuffer.capacity();
        byte dst[] = new byte[max_length];
        while (max_length<remaining) {
            tempBuffer.get(dst);
            outputBuffer.put(dst);
            flush();
            numFlushedstreamItem += frameStreamItemCount++; 
            remaining = tempBuffer.remaining();
        }
        dst = new byte[remaining];
        tempBuffer.get(dst);
        outputBuffer.put(dst);
        
    }

    private void writeKBAStreamDocString(KBAStreamDocument kbaStreamItem) throws IOException {
        String stream_item = kbaStreamItem.toAdmEquivalent() + "\n";
        streamItemCount++; 
        byte[] b = stream_item.getBytes();
        if (outputBuffer.position() + b.length > outputBuffer.limit()) {
            flush();
            numFlushedstreamItem += frameStreamItemCount;
            
            if (b.length <= outputBuffer.limit()) {
                frameStreamItemCount = 0;
                outputBuffer.put(b);
            } else 
                flushBufferFrames(b);
                
        } else {
            outputBuffer.put(b);
        }
        frameStreamItemCount++;
    }

    public int getNumFlushedstreamItem() {
        return numFlushedstreamItem;
    }

    private void flush() throws IOException {
        outputBuffer.flip();
        os.write(outputBuffer.array(), 0, outputBuffer.limit());
        outputBuffer.position(0);
        outputBuffer.limit(32 * 1024);
    }

    public boolean setNextRecordBatch(int numStreamItemsInBatch) throws IOException {
        boolean moreData = kbaStreamIterator.hasNext();
        if (!moreData) {
            if (outputBuffer.position() > 0) {
                flush();
            }
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Reached end of batch. KBA Stream Count: [" + partition + "]" + streamItemCount);
            }
            
            
            return false;
        } else {
            int count = 0;
            while (count < numStreamItemsInBatch && kbaStreamIterator.hasNext()) {
                writeKBAStreamDocString(kbaStreamIterator.next());
                count++;
            }
            return true;
        }
    }

    public void resetOs(OutputStream os) {
        this.os = os;
    }
}