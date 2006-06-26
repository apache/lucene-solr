/** 
 * Copyright 2004 The Apache Software Foundation 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License. 
 */ 
 
package org.apache.lucene.gdata.storage; 
 
import java.security.MessageDigest; 
import java.security.NoSuchAlgorithmException; 
import java.security.SecureRandom; 
import java.util.concurrent.ArrayBlockingQueue; 
import java.util.concurrent.BlockingQueue; 
 
import org.apache.commons.logging.Log; 
import org.apache.commons.logging.LogFactory; 
 
/** 
 * This is the main entry ID generator to generate unique ids for each entry. 
 * The Generator uses {@link java.security.SecureRandom} Numbers and the 
 * {@link java.lang.System#currentTimeMillis()} to create a semi-unique sting; 
 * The string will be digested by a {@link java.security.MessageDigest} which 
 * returns a byte array. The generator encodes the byte array as a hex string. 
 * <p> 
 * The generated Id's will cached in a 
 * {@link java.util.concurrent.BlockingQueue} and reproduced if an id has been 
 * removed. 
 * </p> 
 *  
 * @author Simon Willnauer 
 *  
 */ 
public class IDGenerator { 
    private final SecureRandom secureRandom; 
 
    private final MessageDigest mdigest; 
 
    private final BlockingQueue<String> blockingQueue; 
 
    private Thread runner; 
 
    private static final int DEFAULT_CAPACITY = 10; 
 
    protected static final Log LOGGER = LogFactory.getLog(IDGenerator.class);

    private static final String RUNNER_THREAD_NAME = "GDATA-ID Generator"; 
 
    /** 
     * Constructs a new ID generator. with a fixed capacity of prebuild ids. The 
     * default capacity is 10. Every given parameter less than 10 will be 
     * ignored. 
     *  
     * @param capacity - 
     *            capacity of the prebuild id queue 
     * @throws NoSuchAlgorithmException - 
     *             if the algorithm does not exist 
     */ 
    public IDGenerator(int capacity) throws NoSuchAlgorithmException { 
 
        this.secureRandom = SecureRandom.getInstance("SHA1PRNG"); 
        this.mdigest = MessageDigest.getInstance("SHA-1"); 
        this.blockingQueue = new ArrayBlockingQueue<String>( 
                (capacity < DEFAULT_CAPACITY ? DEFAULT_CAPACITY : capacity), 
                false); 
        startIDProducer(); 
 
    } 
 
    /** 
     * This method takes a gnerated id from the IDProducer queue and retruns it. 
     * If no ID is available this method will wait until an ID is produced. This 
     * implementation is thread-safe. 
     *  
     * @return a UID 
     * @throws InterruptedException - 
     *             if interrupted while waiting 
     */ 
    public String getUID() throws InterruptedException { 
        return this.blockingQueue.take(); 
    } 
 
    private void startIDProducer() { 
        if (this.runner == null) { 
            UIDProducer producer = new UIDProducer(this.blockingQueue, 
                    this.secureRandom, this.mdigest); 
            this.runner = new Thread(producer);
            this.runner.setDaemon(true);
            this.runner.setName(RUNNER_THREAD_NAME);
            this.runner.start(); 
        } 
    } 
 
    /** 
     * @return the current size of the queue 
     */ 
    public int getQueueSize() { 
        return this.blockingQueue.size(); 
    } 
 
    /** 
     * Stops the id-producer 
     */ 
    public void stopIDGenerator() { 
        this.runner.interrupt(); 
    } 
 
    private class UIDProducer implements Runnable { 
        SecureRandom random; 
 
        BlockingQueue<String> queue; 
 
        MessageDigest digest; 
 
        UIDProducer(BlockingQueue<String> queue, SecureRandom random, 
                MessageDigest digest) { 
            this.queue = queue; 
            this.random = random; 
            this.digest = digest; 
 
        } 
 
        /** 
         * @see java.lang.Runnable#run() 
         */ 
        public void run() { 
 
            while (true) { 
                try { 
                    this.queue.put(produce()); 
                } catch (InterruptedException e) { 
                    LOGGER 
                            .warn("UIDProducer has been interrupted -- runner is going down"); 
                    return; 
                } 
            } 
 
        } 
 
        private String produce() { 
            String randomNumber = new Integer(this.random.nextInt()).toString(); 
            byte[] byteResult = this.digest.digest(randomNumber.getBytes()); 
            return hexEncode(byteResult); 
        } 
 
    } 
 
    /** 
     * Encodes a given byte array into a hex string. 
     *  
     * @param input - 
     *            the byte array to encode 
     * @return hex string representation of the given byte array 
     */ 
    static String hexEncode(byte[] input) { 
        StringBuffer result = new StringBuffer(); 
        char[] digits = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 
                'a', 'b', 'c', 'd', 'e', 'f' }; 
        for (int idx = 0; idx < input.length; ++idx) { 
            byte b = input[idx]; 
            result.append(digits[(b & 0xf0) >> 4]); 
            result.append(digits[b & 0x0f]); 
        } 
        return result.toString(); 
    } 
} 
