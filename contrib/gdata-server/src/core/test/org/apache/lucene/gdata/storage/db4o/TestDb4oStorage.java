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
package org.apache.lucene.gdata.storage.db4o;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.TestCase;

import org.apache.lucene.gdata.data.GDataAccount;
import org.apache.lucene.gdata.data.ServerBaseEntry;
import org.apache.lucene.gdata.data.ServerBaseFeed;
import org.apache.lucene.gdata.server.registry.ProvidedServiceConfig;
import org.apache.lucene.gdata.storage.ModificationConflictException;
import org.apache.lucene.gdata.storage.Storage;
import org.apache.lucene.gdata.storage.StorageController;
import org.apache.lucene.gdata.storage.StorageException;
import org.apache.lucene.gdata.storage.lucenestorage.StorageEntryWrapper.StorageOperation;
import org.apache.lucene.gdata.utils.MultiThreadEntryStub;
import org.apache.lucene.gdata.utils.Visitor;

import com.db4o.ObjectContainer;
import com.db4o.ObjectSet;
import com.db4o.query.Query;
import com.google.gdata.data.BaseEntry;
import com.google.gdata.data.BaseFeed;
import com.google.gdata.data.DateTime;
import com.google.gdata.data.PlainTextConstruct;

public class TestDb4oStorage extends TestCase {
    private static final String FEEDID = "myFeed";

    private static final String ACCOUNTNAME = "myAccount";

    private static final String SERVICENAME = "myService";

    DB4oController controller;

    static volatile boolean fail = false;

    protected void setUp() throws Exception {
        this.controller = new DB4oController();
        this.controller.setContainerPoolSize(2);
        this.controller.setFilePath("test.yap");
        this.controller.setRunAsServer(true);
        this.controller.setPassword("");
        this.controller.setUser("");
        this.controller.setUseWeakReferences(true);
        this.controller.setPort(0);
        this.controller.initialize();
        this.controller.visiteInitialize();
        clearDB();
    }

    protected void tearDown() throws Exception {
        clearDB();
        fail = false;
        this.controller.getStorage().close();
        this.controller.visiteDestroy();
        this.controller.destroy();
        File dbFile = new File("test.yap");
        assertTrue(dbFile.delete());
    }

    private void clearDB() {
        ObjectContainer container = this.controller.releaseContainer();
        ObjectSet set = container.get(new Object());

        for (Object object : set) {
            container.delete(object);
        }
        container.ext().purge();
        container.close();
    }

    ObjectContainer getContainer() {
        return this.controller.releaseContainer();
    }

   
    /*
     * Test method for
     * 'org.apache.lucene.gdata.storage.db4o.DB4oStorage.storeEntry(ServerBaseEntry)'
     */
    @SuppressWarnings("unchecked")
    public void testStoreEntry() throws StorageException {
        Storage storage = this.controller.getStorage();
        try {
            ServerBaseEntry e = createServerBaseEntry();
            storage.storeEntry(e);
            fail("excption exp. for feed for the entry");
        } catch (StorageException e) {
            //
        }

        try {

            storage.storeEntry(null);
            fail("entry is null");
        } catch (StorageException e) {
            //
        }
        ServerBaseEntry exEntry = new ServerBaseEntry();
        exEntry.setFeedId("some");
        try {

            storage.storeEntry(exEntry);
            fail("entry id is null");
        } catch (StorageException e) {
            //
        }
        exEntry.setId("someID");
        exEntry.setFeedId(null);
        try {

            storage.storeEntry(exEntry);
            fail("feed id is null");
        } catch (StorageException e) {
            //
        }

        storeServerBaseFeed();
        ServerBaseEntry e = createServerBaseEntry();
        storage.storeEntry(e);
        ServerBaseEntry e1 = createServerBaseEntry();
        storage.storeEntry(e1);

        storage = this.controller.getStorage();
        Query query = getContainer().query();
        query.constrain(BaseEntry.class);
        query.descend("id").constrain(e.getId());
        ObjectSet resultSet = query.execute();
        assertEquals(1, resultSet.size());
        BaseEntry storedEntry = (BaseEntry) resultSet.next();
        assertEquals("1", storedEntry.getVersionId());

        ServerBaseFeed bFeed = new ServerBaseFeed();
        bFeed.setItemsPerPage(25);
        bFeed.setId(FEEDID);
        bFeed.setStartIndex(1);
        bFeed.setServiceType(SERVICENAME);
        BaseFeed<BaseFeed, BaseEntry> feed = storage.getFeed(bFeed);
        assertEquals(2, feed.getEntries().size());
        assertEquals(e.getId(), feed.getEntries().get(1).getId()); // last post
        // ->
        // previously
        // created
        assertEquals(e1.getId(), feed.getEntries().get(0).getId()); // first pos
        // -> last
        // created
        assertEquals(feed.getUpdated(), feed.getEntries().get(0).getUpdated());
    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.storage.db4o.DB4oStorage.deleteEntry(ServerBaseEntry)'
     */
    public void testDeleteEntry() throws StorageException, InterruptedException {
        ObjectContainer container = getContainer();
        storeServerBaseFeed();
        Storage storage = this.controller.getStorage();
        
        try {

            storage.deleteEntry(null);
            fail("entry is null");
        } catch (StorageException e) {
            //
        }
        ServerBaseEntry exEntry = new ServerBaseEntry();
        exEntry.setFeedId("some");
        try {

            storage.deleteEntry(exEntry);
            fail("entry id is null");
        } catch (StorageException e) {
            //
        }
        exEntry.setId("someID");
        exEntry.setFeedId(null);
        try {

            storage.storeEntry(exEntry);
            fail("feed id is null");
        } catch (StorageException e) {
            //
        }
        
        
        ServerBaseEntry e = createServerBaseEntry();
        storage.storeEntry(e);
        ServerBaseEntry e1 = createServerBaseEntry();
        storage.storeEntry(e1);

        storage.deleteEntry(e);

        container.close();
        container = getContainer();
        Query query = container.query();
        query.constrain(BaseEntry.class);
        query.descend("id").constrain(e.getId());
        ObjectSet resultSet = query.execute();
        assertEquals(0, resultSet.size());

        // #### test version matching
        ServerBaseEntry eVersion = createServerBaseEntry();
        storage.storeEntry(eVersion);
        eVersion.setVersion(33);
        try {
            storage.deleteEntry(eVersion);
            fail("version does not match");
        } catch (Exception ex) {
            // TODO: handle exception
        }
        try {
            storage.deleteEntry(null);
            fail("entry id is null");
        } catch (Exception ex) {
            // TODO: handle exception
        }
        storage = this.controller.getStorage();
        storage.deleteEntry(e1);
        container.close();
        container = getContainer();
        query = container.query();
        query.constrain(BaseEntry.class);
        query.descend("id").constrain(e1.getId());
        resultSet = query.execute();
        assertEquals(0, resultSet.size());

        // ############ test concurrency

        // ############ test concurrency
        Object monitor = new Object();
        AtomicBoolean reached = new AtomicBoolean(false);
        MultiThreadEntryStub concuEntry = new MultiThreadEntryStub();
        concuEntry.setId(System.currentTimeMillis() + "");
        ProvidedServiceConfig conf = new ProvidedServiceConfig();
        conf.setName(SERVICENAME);
        concuEntry.setServiceConfig(conf);
        concuEntry.setUpdated(DateTime.now());
        concuEntry.setFeedId(FEEDID);

        storage = this.controller.getStorage();

        storage.storeEntry(concuEntry);
        storage.close();
        concuEntry.acceptGetVersionVisitor(getMonitorVisitor(monitor, reached));

        Thread t1 = getDelThread(controller, concuEntry, false);

        Thread t2 = getDelThread(controller, concuEntry, true);
        t1.start();
        /*
         * Wait active -- not nice but works fine here wait until thread parked
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
         * Wait active -- not nice but works fine here wake up the waiting
         * thread
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

        container.close();

    }

    private Visitor getMonitorVisitor(final Object monitor,
            final AtomicBoolean reached) {
        /*
         * The executing thread stops at a defined position while holding the
         * semaphore inside the storageImpl
         */
        return new Visitor() {
            public void execute(Object[] o) {
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

    private Thread getDelThread(StorageController c, ServerBaseEntry e,
            boolean conflictExpected) {
        Thread t1 = new Thread(new Runner(c, e, conflictExpected,
                StorageOperation.DELETE));
        t1.setPriority(Thread.MAX_PRIORITY);
        return t1;
    }

    private Thread getUpdThread(StorageController c, ServerBaseEntry e,
            boolean conflictExpected) {
        Thread t1 = new Thread(new Runner(c, e, conflictExpected,
                StorageOperation.UPDATE));
        t1.setPriority(Thread.MAX_PRIORITY);
        return t1;
    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.storage.db4o.DB4oStorage.updateEntry(ServerBaseEntry)'
     */
    public void testUpdateEntry() throws StorageException, InterruptedException {
        storeServerBaseFeed();
        Storage storage = this.controller.getStorage();
        ServerBaseEntry exEntry = new ServerBaseEntry();
        

        try {

            storage.updateEntry(null);
            fail("entry is null");
        } catch (StorageException e) {
            //
        }

        try {

            storage.updateEntry(exEntry);
            fail("entry id is null");
        } catch (StorageException e) {
            //
        }
        exEntry.setId("someID");
        try {

            storage.updateEntry(exEntry);
            fail("feed id is null");
        } catch (StorageException e) {
            //
        }
        
        
        
        ServerBaseEntry e = createServerBaseEntry();
        ServerBaseEntry e1 = createServerBaseEntry();
        try {
            storage.updateEntry(e);
            fail("entry does not exist");
        } catch (StorageException ex) {
            ex.printStackTrace();
        }
        storage.storeEntry(e);

        storage = this.controller.getStorage();

        storage.storeEntry(e1);
        ServerBaseEntry e2 = createServerBaseEntry();
        e2.setId(e.getId());
        e2.setTitle(new PlainTextConstruct("new"));
        e2.setUpdated(DateTime.now());
        storage.updateEntry(e2);
        ObjectContainer container = getContainer();
        Query query = container.query();
        query.constrain(BaseEntry.class);
        query.descend("id").constrain(e.getId());
        ObjectSet resultSet = query.execute();
        assertEquals(1, resultSet.size());
        BaseEntry result = (BaseEntry) resultSet.next();
        assertEquals("new", result.getTitle().getPlainText());
        assertEquals("2", result.getVersionId());

        ServerBaseFeed bFeed = new ServerBaseFeed();
        bFeed.setItemsPerPage(25);
        bFeed.setId(FEEDID);
        bFeed.setStartIndex(1);
        bFeed.setServiceType(SERVICENAME);
        storage = this.controller.getStorage();
        BaseFeed<BaseFeed, BaseEntry> feed = storage.getFeed(bFeed);

        assertEquals(2, feed.getEntries().size());
        assertEquals(e.getId(), feed.getEntries().get(0).getId());
        assertEquals(feed.getUpdated(), feed.getEntries().get(0).getUpdated());

        storage = this.controller.getStorage();
        storage.storeEntry(e);

        e2.setVersion(5);
        try {
            storage.updateEntry(e2);
            fail("version does not match");
        } catch (Exception ex) {
            // TODO: handle exception
        }

        // ############ test concurrency
        Object monitor = new Object();
        AtomicBoolean reached = new AtomicBoolean(false);

        MultiThreadEntryStub concuEntry = new MultiThreadEntryStub();
        concuEntry.setId(System.currentTimeMillis() + "");
        ProvidedServiceConfig conf = new ProvidedServiceConfig();
        conf.setName(SERVICENAME);
        concuEntry.setServiceConfig(conf);
        concuEntry.setUpdated(DateTime.now());
        concuEntry.setFeedId(FEEDID);

        storage = this.controller.getStorage();

        storage.storeEntry(concuEntry);
        storage.close();
        concuEntry.acceptGetEntryVisitor(getMonitorVisitor(monitor, reached));

        Thread t1 = getUpdThread(controller, concuEntry, false);

        Thread t2 = getUpdThread(controller, concuEntry, true);
        t1.start();
        /*
         * Wait active -- not nice but works fine here wait until thread parked
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
         * Wait active -- not nice but works fine here wake up the waiting
         * thread
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
        container.close();
    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.storage.db4o.DB4oStorage.getFeed(ServerBaseFeed)'
     */
    public void testGetFeed() throws StorageException {
        storeServerBaseFeed();
        Storage storage = this.controller.getStorage();
        ServerBaseFeed feed = new ServerBaseFeed();
        feed.setItemsPerPage(25);
        feed.setStartIndex(1);
        feed.setServiceType(SERVICENAME);
        try{
        storage.getFeed(feed);
        fail("feedid is null");
        }catch (StorageException e) {
            // 
        }
        
        feed.setId(FEEDID);
        BaseFeed result = storage.getFeed(feed);
        assertNotNull(result);
        assertEquals(0, result.getEntries().size());
        List<String> idlist = new ArrayList<String>(30);
        ServerBaseEntry e1 = null;
        for (int i = 0; i < 30; i++) {
            e1 = createServerBaseEntry();
            storage.storeEntry(e1);
            idlist.add(0, e1.getId());
        }
        String firstId = e1.getId();

        storage = this.controller.getStorage();
        result = storage.getFeed(feed);
        assertNotNull(result);
        assertEquals(25, result.getEntries().size());
        for (int i = 0; i < 25; i++) {
            assertEquals(idlist.get(i),
                    ((BaseEntry) result.getEntries().get(i)).getId());
        }

        storage = this.controller.getStorage();
        feed.setItemsPerPage(5);
        result = storage.getFeed(feed);
        assertNotNull(result);
        assertEquals(5, result.getEntries().size());
        for (int i = 0; i < 5; i++) {
            assertEquals(idlist.get(i),
                    ((BaseEntry) result.getEntries().get(i)).getId());
        }

        storage = this.controller.getStorage();
        feed.setItemsPerPage(1);
        feed.setStartIndex(1);
        result = storage.getFeed(feed);
        assertNotNull(result);
        assertEquals(1, result.getEntries().size());

        assertEquals(idlist.get(0), ((BaseEntry) result.getEntries().get(0))
                .getId());

        storage = this.controller.getStorage();
        feed.setItemsPerPage(50);
        feed.setStartIndex(28);
        result = storage.getFeed(feed);
        assertNotNull(result);
        assertEquals(3, result.getEntries().size());

        assertEquals(idlist.get(27), ((BaseEntry) result.getEntries().get(0))
                .getId());
        assertEquals(idlist.get(28), ((BaseEntry) result.getEntries().get(1))
                .getId());
        assertEquals(idlist.get(29), ((BaseEntry) result.getEntries().get(2))
                .getId());

        storage = this.controller.getStorage();
        feed.setItemsPerPage(50);
        feed.setStartIndex(30);
        result = storage.getFeed(feed);
        assertNotNull(result);
        assertEquals(1, result.getEntries().size());

        assertEquals(idlist.get(29), ((BaseEntry) result.getEntries().get(0))
                .getId());

        // assertNotSame(firstId,((BaseEntry)result.getEntries().get(0)).getId());
    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.storage.db4o.DB4oStorage.getEntry(ServerBaseEntry)'
     */
    public void testGetEntry() throws StorageException {
        storeServerBaseFeed();
        Storage storage = this.controller.getStorage();
        ServerBaseEntry exEntry = createServerBaseEntry();
        exEntry.setId(null);
        try{
        storage.getEntry(exEntry);
        fail("id is null");
        }catch (StorageException e) {

        }
        ServerBaseEntry e = createServerBaseEntry();
        storage.storeEntry(e);
        ServerBaseEntry e1 = createServerBaseEntry();
        storage.storeEntry(e1);
        
        storage = this.controller.getStorage();
        BaseEntry result = storage.getEntry(e);
        assertNotNull(result);
        assertEquals(e.getId(), result.getId());
        try {
            e1.setId("hello");
            result = storage.getEntry(e1);
            fail("no such entry");
        } catch (StorageException ex) {
            ex.printStackTrace();
        }

    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.storage.db4o.DB4oStorage.storeAccount(GDataAccount)'
     */
    public void testStoreAccount() throws StorageException {
        GDataAccount account = new GDataAccount();
        account.setName("simon");
        account.setPassword("somepass");
        Storage storage = this.controller.getStorage();
        storage.storeAccount(account);
        ObjectContainer container = getContainer();
        Query q = container.query();
        q.constrain(GDataAccount.class);
        q.descend("name").constrain(account.getName());
        ObjectSet set = q.execute();
        assertEquals(1, set.size());
        assertEquals(account.getPassword(), ((GDataAccount) set.next())
                .getPassword());
        try {
            storage.storeAccount(account);
            fail("Account already stored");
        } catch (Exception e) {

        }
        container.close();
    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.storage.db4o.DB4oStorage.updateAccount(GDataAccount)'
     */
    public void testUpdateAccount() throws StorageException {
        GDataAccount account = new GDataAccount();
        account.setName("simon");
        account.setPassword("somepass");

        Storage storage = this.controller.getStorage();
        try {
            storage.updateAccount(account);
            fail("Account does not exist");
        } catch (Exception e) {
            //
        }
        try {
            storage.updateAccount(null);
            fail("Account is null");
        } catch (Exception e) {
            //
        }
        storage.storeAccount(account);
        ObjectContainer container = getContainer();
        Query q = container.query();
        q.constrain(GDataAccount.class);
        q.descend("name").constrain(account.getName());
        ObjectSet set = q.execute();
        assertEquals(1, set.size());
        assertEquals(account.getPassword(), ((GDataAccount) set.next())
                .getPassword());
        account = new GDataAccount();
        account.setName("simon");
        account.setPassword("newPass");
        storage.updateAccount(account);
        container.close();
        container = getContainer();
        q = container.query();
        q.constrain(GDataAccount.class);
        q.descend("name").constrain(account.getName());
        set = q.execute();
        assertEquals(1, set.size());
        assertEquals(account.getPassword(), ((GDataAccount) set.next())
                .getPassword());
        container.close();
    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.storage.db4o.DB4oStorage.deleteAccount(String)'
     */
    public void testDeleteAccount() throws StorageException {
        GDataAccount account = new GDataAccount();
        account.setName("simon");
        account.setPassword("somepass");
        Storage storage = this.controller.getStorage();
        storage.storeAccount(account);
        ObjectContainer container = getContainer();
        Query q = container.query();
        q.constrain(GDataAccount.class);
        q.descend("name").constrain(account.getName());
        ObjectSet set = q.execute();
        assertEquals(1, set.size());

        storage.deleteAccount(account.getName());
        container.close();
        container = getContainer();
        q = container.query();
        q.constrain(GDataAccount.class);
        q.descend("name").constrain(account.getName());
        set = q.execute();
        assertEquals(0, set.size());
        try {
            storage.deleteAccount("notstored");
            fail("account not stored");
        } catch (Exception e) {
            // 
        }
        try {
            storage.deleteAccount(null);
            fail("name is null");
        } catch (Exception e) {
            // 
        }
        container.close();
    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.storage.db4o.DB4oStorage.storeFeed(ServerBaseFeed,
     * String)'
     */
    public void testStoreFeed() throws StorageException {
        ObjectContainer container = getContainer();
        ServerBaseFeed feed = new ServerBaseFeed();
        feed.setId(FEEDID);
        ProvidedServiceConfig conf = new ProvidedServiceConfig();
        conf.setName(SERVICENAME);
        feed.setServiceConfig(conf);

        Storage storage = this.controller.getStorage();
        try {
            storage.storeFeed(feed, ACCOUNTNAME);
            fail("no accoutn stored");
        } catch (Exception e) {
            // 
        }
        GDataAccount account = new GDataAccount();
        account.setName(ACCOUNTNAME);
        account.setPassword("somePass");
        container.set(account);
        container.commit();
        container.close();
        storage.storeFeed(feed, ACCOUNTNAME);

        container = getContainer();
        Query query = container.query();
        query.constrain(ServerBaseFeed.class);
        query.descend("feed").descend("id").constrain(FEEDID);
        ObjectSet set = query.execute();
        assertEquals(1, set.size());

        assertEquals(feed.getId(), ((ServerBaseFeed) set.next()).getId());
        container.close();

    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.storage.db4o.DB4oStorage.deleteFeed(String)'
     */
    public void testDeleteFeed() throws StorageException {

        ServerBaseFeed feed = new ServerBaseFeed();
        feed.setId(FEEDID);
        GDataAccount account = new GDataAccount();
        account.setName(ACCOUNTNAME);
        account.setPassword("somePass");
        ObjectContainer container = getContainer();
        container.set(account);
        container.commit();
        container.close();
        Storage storage = this.controller.getStorage();
        ProvidedServiceConfig conf = new ProvidedServiceConfig();
        conf.setName(SERVICENAME);
        feed.setServiceConfig(conf);
        storage.storeFeed(feed, ACCOUNTNAME);

        storage.deleteFeed(FEEDID);
        container = getContainer();
        Query query = container.query();
        query.constrain(ServerBaseFeed.class);
        query.descend("feed").descend("id").constrain(FEEDID);
        ObjectSet set = query.execute();
        assertEquals(0, set.size());

        query = getContainer().query();
        query.constrain(BaseFeed.class);
        query.descend("id").constrain(FEEDID);
        set = query.execute();
        assertEquals(0, set.size());
        container.close();
    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.storage.db4o.DB4oStorage.getServiceForFeed(String)'
     */
    public void testGetServiceForFeed() throws StorageException {
        ServerBaseFeed feed = new ServerBaseFeed();
        ProvidedServiceConfig conf = new ProvidedServiceConfig();
        conf.setName(SERVICENAME);
        feed.setServiceConfig(conf);
        feed.setId(FEEDID);
        GDataAccount account = new GDataAccount();
        account.setName(ACCOUNTNAME);
        account.setPassword("somePass");
        ObjectContainer container = getContainer();
        container.set(account);
        container.commit();
        container.close();
        Storage storage = this.controller.getStorage();
        storage.storeFeed(feed, ACCOUNTNAME);

        assertEquals(SERVICENAME, storage.getServiceForFeed(FEEDID));
        try {
            storage.getServiceForFeed(null);
            fail("ID is null");
        } catch (Exception e) {
            // 
        }

        try {
            storage.getServiceForFeed("someOtherId");
            fail("feed for id is not stored");
        } catch (Exception e) {
            // 
        }

    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.storage.db4o.DB4oStorage.getAccount(String)'
     */
    public void testGetAccount() throws StorageException {
        GDataAccount account = new GDataAccount();
        account.setName(ACCOUNTNAME);
        account.setPassword("somePass");
        ObjectContainer container = getContainer();
        container.set(account);
        container.commit();
        container.close();

        Storage storage = this.controller.getStorage();
        assertNotNull(storage.getAccount(ACCOUNTNAME));
        assertEquals(account.getPassword(), storage.getAccount(ACCOUNTNAME)
                .getPassword());
        try {
            storage.getAccount(null);
            fail("accountname is null");
        } catch (Exception e) {
            // 
        }
        try {
            storage.getAccount("someOtherAccount");
            fail("accountname is not stored");
        } catch (Exception e) {
            // 
        }
    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.storage.db4o.DB4oStorage.updateFeed(ServerBaseFeed,
     * String)'
     */
    public void testUpdateFeed() throws StorageException {
        ObjectContainer container = getContainer();
        ServerBaseFeed feed = new ServerBaseFeed();
        ProvidedServiceConfig conf = new ProvidedServiceConfig();
        conf.setName(SERVICENAME);
        feed.setId(FEEDID);
        feed.setServiceConfig(conf);
        Storage storage = this.controller.getStorage();
        GDataAccount account = new GDataAccount();
        account.setName(ACCOUNTNAME);
        account.setPassword("somePass");
        container.set(account);
        container.commit();
        container.close();
        storage.storeFeed(feed, ACCOUNTNAME);
        assertNull(feed.getTitle());
        ServerBaseFeed feedU = new ServerBaseFeed();
        feedU.setServiceConfig(conf);
        feedU.setId(FEEDID);
        feedU.setTitle(new PlainTextConstruct("someText"));
        feedU.setServiceType(SERVICENAME);

        storage.updateFeed(feedU, ACCOUNTNAME);
        ServerBaseFeed requestFeed = new ServerBaseFeed();
        requestFeed.setId(FEEDID);
        requestFeed.setServiceType(SERVICENAME);
        assertNotNull(storage.getFeed(requestFeed));
        assertEquals(feedU.getTitle(), storage.getFeed(requestFeed).getTitle());
        try {
            storage.updateFeed(null, ACCOUNTNAME);
            fail("feed is null");
        } catch (Exception e) {
            // 
        }
        try {
            storage.updateFeed(feedU, null);
            fail("accountname is null");
        } catch (Exception e) {
            // 
        }
        try {
            feedU.setServiceType(null);
            storage.updateFeed(feedU, ACCOUNTNAME);
            fail("servicetype is null");
        } catch (Exception e) {
            // 
        }

    }

    private static ServerBaseEntry createServerBaseEntry() {
        ServerBaseEntry e = new ServerBaseEntry();
        e.setId(System.currentTimeMillis() + "");
        ProvidedServiceConfig conf = new ProvidedServiceConfig();
        conf.setName(SERVICENAME);
        e.setServiceConfig(conf);
        e.setUpdated(DateTime.now());
        e.setFeedId(FEEDID);
        try {
            Thread.sleep(2);
        } catch (InterruptedException e1) {

            e1.printStackTrace();
        }
        return e;
    }

    private ServerBaseFeed storeServerBaseFeed() {
        ServerBaseFeed f = new ServerBaseFeed();
        ProvidedServiceConfig conf = new ProvidedServiceConfig();
        conf.setName(SERVICENAME);
        f.setServiceConfig(conf);
        f.setId(System.currentTimeMillis() + "");
        f.setId(FEEDID);
        f.setUpdated(DateTime.now());
        ObjectContainer con = this.controller.releaseContainer();
        con.set(f);
        con.commit();

        con.close();
        return f;
    }

    static class Runner implements Runnable {
        Storage s;

        StorageController c;

        ServerBaseEntry e;

        boolean expConf;

        StorageOperation op;

        public Runner(StorageController c, ServerBaseEntry e,
                boolean expectConflict, StorageOperation op) {

            this.c = c;

            this.e = e;
            this.expConf = expectConflict;
            this.op = op;

        }

        public void run() {
            try {
                ((DB4oController) this.c).visiteInitialize();
                this.s = this.c.getStorage();
            } catch (StorageException e1) {

                e1.printStackTrace();
            }
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
            } finally {
                ((DB4oController) this.c).visiteDestroy();
            }

        }
    }

    public void testFeedLastModified() {
        ServerBaseFeed feed = storeServerBaseFeed();
        Storage s = this.controller.getStorage();
        assertEquals(feed.getUpdated().getValue(), s
                .getFeedLastModified(FEEDID).longValue());
        try {
            s.getFeedLastModified(null);
            fail("id is null");
        } catch (StorageException e) {

        }
        try {
            s.getFeedLastModified("someOtherid");
            fail("no such feed");
        } catch (StorageException e) {

        }

    }

    public void testEntryLastModified() {
        ServerBaseFeed feed = storeServerBaseFeed();
        Storage s = this.controller.getStorage();
        ServerBaseEntry en = createServerBaseEntry();
        s.storeEntry(en);
        assertEquals(en.getUpdated().getValue(), s.getEntryLastModified(
                en.getId(), FEEDID).longValue());
        try {
            s.getEntryLastModified(null, null);
            fail("id is null");
        } catch (StorageException e) {

        }
        try {
            s.getEntryLastModified("someOtherid", "notinstorage");
            fail("no such Entry");
        } catch (StorageException e) {

        }

    }

}
