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
package org.apache.lucene.gdata.storage.lucenestorage;

import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.TestCase;

import org.apache.lucene.gdata.data.ServerBaseEntry;
import org.apache.lucene.gdata.server.registry.ComponentType;
import org.apache.lucene.gdata.server.registry.GDataServerRegistry;
import org.apache.lucene.gdata.server.registry.ProvidedService;
import org.apache.lucene.gdata.storage.ModificationConflictException;
import org.apache.lucene.gdata.storage.Storage;
import org.apache.lucene.gdata.storage.StorageController;
import org.apache.lucene.gdata.storage.StorageException;
import org.apache.lucene.gdata.storage.lucenestorage.StorageEntryWrapper.StorageOperation;
import org.apache.lucene.gdata.utils.MultiThreadEntryStub;
import org.apache.lucene.gdata.utils.ProvidedServiceStub;
import org.apache.lucene.gdata.utils.Visitor;

import com.google.gdata.data.DateTime;

public class TestStorageImplementation extends TestCase {
    private static GDataServerRegistry reg = null;

    private Storage storage;

    public static boolean fail = false;

    protected void setUp() throws Exception {

        if (reg == null) {
            reg = GDataServerRegistry.getRegistry();
            if(reg.lookup(StorageController.class,ComponentType.STORAGECONTROLLER)!= null);
                reg.destroy();
            reg.registerComponent(StorageCoreControllerStub.class,null);

        }
        this.storage = reg.lookup(StorageController.class,
                ComponentType.STORAGECONTROLLER).getStorage();
    }

    protected void tearDown() throws Exception {
        this.storage.close();
        fail = false;
    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.storage.lucenestorage.StorageImplementation.storeEntry(ServerBaseEntry)'
     */
    public void testStoreEntry() {

        try {
            this.storage.storeEntry(null);
            fail("entry is null");
        } catch (StorageException e) {
            //
        }
        ServerBaseEntry entry = new ServerBaseEntry();
        entry.setServiceConfig(new ProvidedServiceStub());

        try {
            this.storage.storeEntry(entry);
            fail("feed is null");
        } catch (StorageException e) {
            //
        }

        entry.setFeedId("someID");
        try {
            this.storage.storeEntry(entry);

        } catch (StorageException e1) {
            fail("unexpected exception");

        }
        entry.setServiceConfig(null);
        try {
            this.storage.storeEntry(entry);
            fail("no service config");
        } catch (StorageException e) {

        }
        entry.setVersion(5);
        try {
            this.storage.storeEntry(entry);
            fail("version is greater than 1");
        } catch (StorageException e) {

        }

    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.storage.lucenestorage.StorageImplementation.deleteEntry(ServerBaseEntry)'
     */
    public void testDeleteEntry() throws InterruptedException {
        try {
            this.storage.storeEntry(null);
            fail("entry is null");
        } catch (StorageException e) {
            //
        }
        ServerBaseEntry entry = new ServerBaseEntry();
        entry.setServiceConfig(new ProvidedServiceStub());
        entry.setId("someID");
        entry.setFeedId("someID");
        try {
            this.storage.storeEntry(entry);

        } catch (StorageException e) {
            fail("unexpected exception");
            //
        }
        entry.setFeedId(null);
        try {
            this.storage.deleteEntry(entry);
            fail("feed is null");
        } catch (StorageException e) {
            //
        }

        entry.setFeedId("someID");
        try {
            this.storage.deleteEntry(entry);

        } catch (StorageException e1) {
            e1.printStackTrace();
            fail("unexpected exception");

        }
        entry.setFeedId("someID");
        
        try {
            this.storage.deleteEntry(entry);

        } catch (StorageException e1) {
            e1.printStackTrace();
            fail("unexpected exception");

        }
        
        
        
        Object monitor = new Object();
        AtomicBoolean reached = new AtomicBoolean(false);

        MultiThreadEntryStub concuEntry = new MultiThreadEntryStub();
        concuEntry.setId(System.currentTimeMillis() + "");
        ProvidedService conf = new ProvidedServiceStub();
        
        concuEntry.setServiceConfig(conf);
        concuEntry.setUpdated(DateTime.now());
        concuEntry.setFeedId("feed");
        this.storage.storeEntry(concuEntry);
        storage.close();
        concuEntry.acceptGetVersionVisitor(getMonitorVisitor(monitor,reached));


        Thread t1 = getDelThread(storage, concuEntry, false);

        Thread t2 = getDelThread(storage, concuEntry, true);
        t1.start();
        /*
         * Wait active -- not nice but works fine here
         * wait until thread parked
         */

        while (true) {
            synchronized (monitor) {
                if (reached.get())
                    break;
                monitor.wait(10);
            }
        }
        t2.start();
        t2.join(800);
        /*
         * Wait active -- not nice but works fine here
         * wake up the waiting thread
         */
        while (true) {
            synchronized (monitor) {
                if (!reached.get())
                    break;
                monitor.notifyAll();
            }
        }
        t1.join(300);
        if (fail)
            fail("thread failed -- see stacktrace");
       
       
    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.storage.lucenestorage.StorageImplementation.updateEntry(ServerBaseEntry)'
     */
    public void testUpdateEntry() throws InterruptedException {
        Object monitor = new Object();
        AtomicBoolean reached = new AtomicBoolean(false);

        MultiThreadEntryStub concuEntry = new MultiThreadEntryStub();
        concuEntry.setId(System.currentTimeMillis() + "");
        ProvidedService conf = new ProvidedServiceStub();
        
        concuEntry.setServiceConfig(conf);
        concuEntry.setUpdated(DateTime.now());
        concuEntry.setFeedId("feed");
        this.storage.storeEntry(concuEntry);
        storage.close();
        concuEntry.acceptGetEntryVisitor(getMonitorVisitor(monitor,reached));


        Thread t1 = getUpdThread(storage, concuEntry, false);

        Thread t2 = getUpdThread(storage, concuEntry, true);
        t1.start();
        /*
         * Wait active -- not nice but works fine here
         * wait until thread parked
         */

        while (true) {
            synchronized (monitor) {
                if (reached.get())
                    break;
                monitor.wait(10);
            }
        }
        t2.start();
        t2.join(800);
        /*
         * Wait active -- not nice but works fine here
         * wake up the waiting thread
         */
        while (true) {
            synchronized (monitor) {
                if (!reached.get())
                    break;
                monitor.notifyAll();
            }
        }
        t1.join(300);
        if (fail)
            fail("thread failed -- see stacktrace");

    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.storage.lucenestorage.StorageImplementation.getFeed(ServerBaseFeed)'
     */
    public void testGetFeed() {

    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.storage.lucenestorage.StorageImplementation.getEntry(ServerBaseEntry)'
     */
    public void testGetEntry() {

    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.storage.lucenestorage.StorageImplementation.close()'
     */
    public void testClose() {

    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.storage.lucenestorage.StorageImplementation.storeAccount(GDataAccount)'
     */
    public void testStoreAccount() {

    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.storage.lucenestorage.StorageImplementation.updateAccount(GDataAccount)'
     */
    public void testUpdateAccount() {

    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.storage.lucenestorage.StorageImplementation.deleteAccount(String)'
     */
    public void testDeleteAccount() {

    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.storage.lucenestorage.StorageImplementation.storeFeed(ServerBaseFeed,
     * String)'
     */
    public void testStoreFeed() {

    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.storage.lucenestorage.StorageImplementation.deleteFeed(String)'
     */
    public void testDeleteFeed() {

    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.storage.lucenestorage.StorageImplementation.updateFeed(ServerBaseFeed,
     * String)'
     */
    public void testUpdateFeed() {

    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.storage.lucenestorage.StorageImplementation.getServiceForFeed(String)'
     */
    public void testGetServiceForFeed() {

    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.storage.lucenestorage.StorageImplementation.getAccount(String)'
     */
    public void testGetAccount() {

    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.storage.lucenestorage.StorageImplementation.getAccountNameForFeedId(String)'
     */
    public void testGetAccountNameForFeedId() {

    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.storage.lucenestorage.StorageImplementation.getEntryLastModified(String,
     * String)'
     */
    public void testGetEntryLastModified() {

    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.storage.lucenestorage.StorageImplementation.getFeedLastModified(String)'
     */
    public void testGetFeedLastModified() {

    }
    static class Runner implements Runnable {
        Storage s;

        StorageController c;

        ServerBaseEntry e;

        boolean expConf;

        StorageOperation op;

        public Runner(Storage s, ServerBaseEntry e,
                boolean expectConflict, StorageOperation op) {
            this.s = s;
            this.e = e;
            this.expConf = expectConflict;
            this.op = op;
        }

        public void run() {
           
            try {
                if (this.op == StorageOperation.DELETE)
                    this.s.deleteEntry(e);
                if (this.op == StorageOperation.UPDATE)
                    this.s.updateEntry(e);
                if (expConf)
                    fail = true;
            } catch (ModificationConflictException ex) {
                if (!expConf)
                    fail = true;
                ex.printStackTrace();

            } catch (StorageException ex) {
                ex.printStackTrace();
                fail = true;
            }

        }
    }
    
    private Visitor getMonitorVisitor(final Object monitor, final AtomicBoolean reached){
        /*
         * The executing thread stops at a defined position while holding the semaphore inside the storageImpl
         */
        return new Visitor(){
          public void execute(Object[] o){
            synchronized (monitor) {
            try {
                reached.set(true);
                monitor.wait();
                reached.set(false);
              
            } catch (InterruptedException e) {
                //                   
            }
            }
          }
        };
    }

    private Thread getDelThread(Storage s, ServerBaseEntry e,
            boolean conflictExpected) {
        Thread t1 = new Thread(new Runner(s, e, conflictExpected,
                StorageOperation.DELETE));
        t1.setPriority(Thread.MAX_PRIORITY);
        return t1;
    }

    private Thread getUpdThread(Storage s, ServerBaseEntry e,
            boolean conflictExpected) {
        Thread t1 = new Thread(new Runner(s, e, conflictExpected,
                StorageOperation.UPDATE));
        t1.setPriority(Thread.MAX_PRIORITY);
        return t1;
    }

}
