package org.apache.lucene.gdata.storage.lucenestorage;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.lucene.gdata.data.GDataAccount;
import org.apache.lucene.gdata.data.ServerBaseEntry;
import org.apache.lucene.gdata.data.ServerBaseFeed;
import org.apache.lucene.gdata.server.registry.ComponentType;
import org.apache.lucene.gdata.server.registry.GDataServerRegistry;
import org.apache.lucene.gdata.server.registry.ProvidedService;
import org.apache.lucene.gdata.storage.StorageController;
import org.apache.lucene.gdata.storage.StorageException;
import org.apache.lucene.gdata.storage.lucenestorage.StorageEntryWrapper.StorageOperation;
import org.apache.lucene.gdata.storage.lucenestorage.util.ReferenceCounter;
import org.apache.lucene.gdata.utils.ProvidedServiceStub;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;

import com.google.gdata.data.BaseEntry;
import com.google.gdata.data.DateTime;
import com.google.gdata.data.Entry;
import com.google.gdata.data.PlainTextConstruct;
import com.google.gdata.data.TextContent;
import com.google.gdata.util.ParseException;

public class TestStorageModifier extends TestCase {
    private StorageModifier modifier;

    private int count = 1;

    private ProvidedService configurator;

    private Directory dir;

    private StorageCoreController controller;

    private static String feedId = "myFeed";

    private static String username = "simon";

    private static String password = "test";
    private static String service = "myService";

    protected void setUp() throws Exception {
        GDataServerRegistry.getRegistry().registerComponent(
                StorageCoreController.class);
        this.configurator = new ProvidedServiceStub();
        GDataServerRegistry.getRegistry().registerService(this.configurator);
        this.controller = (StorageCoreController) GDataServerRegistry
                .getRegistry().lookup(StorageController.class,
                        ComponentType.STORAGECONTROLLER);
        this.modifier = this.controller.getStorageModifier();
        this.dir = this.controller.getDirectory();

    }

    protected void tearDown() throws Exception {
        this.count = 1;
        // destroy all resources
        GDataServerRegistry.getRegistry().destroy();// TODO remove dependency
                                                    // here

    }

    /*
     * Test method for
     * 'org.apache.lucene.storage.lucenestorage.StorageModifier.updateEntry(StroageEntryWrapper)'
     */
    public void testUpdateEntry() throws IOException, InterruptedException,
            ParseException, StorageException {
        testInsertEntry();
        for (int i = 1; i < this.count; i++) {
            Entry e = new Entry();
            e.setId("" + i);
            String insertString = "Hello world" + i;
            e.setTitle(new PlainTextConstruct(insertString));
            ServerBaseEntry en = getServerEntry(e);
            StorageEntryWrapper wrapper = new StorageEntryWrapper(en,
                    StorageOperation.UPDATE);
            this.modifier.updateEntry(wrapper);
            ReferenceCounter<StorageQuery> innerQuery = this.controller
                    .getStorageQuery();
            BaseEntry fetchedEntry = innerQuery.get().singleEntryQuery("" + i,
                    feedId, this.configurator);
            assertEquals("updated Title:", insertString, fetchedEntry
                    .getTitle().getPlainText());
        }
        // double updates
        for (int i = 1; i < this.count; i++) {
            Entry e = new Entry();
            e.setId("" + i);
            String insertString = "Hello world" + i;
            e.setTitle(new PlainTextConstruct(insertString));
            ServerBaseEntry en = getServerEntry(e);
            StorageEntryWrapper wrapper = new StorageEntryWrapper(en,
                    StorageOperation.UPDATE);
            this.modifier.updateEntry(wrapper);

            e = new Entry();
            e.setId("" + i);
            insertString = "Foo Bar" + i;
            e.setTitle(new PlainTextConstruct(insertString));
            en = getServerEntry(e);
            wrapper = new StorageEntryWrapper(en,
                    StorageOperation.UPDATE);
            this.modifier.updateEntry(wrapper);

            ReferenceCounter<StorageQuery> innerQuery = this.controller
                    .getStorageQuery();

            BaseEntry fetchedEntry = innerQuery.get().singleEntryQuery("" + i,
                    feedId, this.configurator);
            assertEquals("updated Title:", insertString, fetchedEntry
                    .getTitle().getPlainText());
        }

    }

    /*
     * Test method for
     * 'org.apache.lucene.storage.lucenestorage.StorageModifier.insertEntry(StroageEntryWrapper)'
     */
    public void testInsertEntry() throws IOException, InterruptedException,
            ParseException, StorageException {

        Thread a = getRunnerThread(this.count);
        a.start();

        Thread b = getRunnerThread((this.count += 10));
        b.start();
        // wait for the first thread to check for the inserted entries
        a.join();  
        try{
        for (int i = 1; i < this.count; i++) {
            ReferenceCounter<StorageQuery> innerQuery = this.controller
                    .getStorageQuery();
            BaseEntry e = innerQuery.get().singleEntryQuery("" + i, feedId,
                    this.configurator);
            assertNotNull(e);
            assertEquals("get entry for id" + i, "" + i, e.getId());

        }
        }finally{
        	/*
        	 * if an exception occures the tread can at least finnish running before the
        	 * controller will be closed in the tearDown method
        	 */
        	 b.join();	
        }
      
       
        ReferenceCounter<StorageQuery> query = this.controller
                .getStorageQuery();

        this.count += 10;
        for (int i = 1; i < this.count; i++) {
            BaseEntry e = query.get().singleEntryQuery("" + i, feedId,
                    this.configurator);
            assertEquals("get entry for id" + i, "" + i, e.getId());
        }

        BaseEntry e = query.get().singleEntryQuery("" + this.count, feedId,
                this.configurator);
        assertNull("not entry for ID", e);
        query.decrementRef();

    }

    /*
     * Test method for
     * 'org.apache.lucene.storage.lucenestorage.StorageModifier.deleteEntry(String)'
     */
    public void testDeleteEntry() throws IOException, InterruptedException,
            ParseException, StorageException {
        testInsertEntry();
        for (int i = 1; i < this.count; i++) {
            if (i % 2 == 0 || i < 10) {
                ServerBaseEntry entry = new ServerBaseEntry();
                entry.setId("" + i);
                entry.setFeedId(feedId);
                this.modifier.deleteEntry(new StorageEntryWrapper(entry,StorageOperation.DELETE));
            }
            ReferenceCounter<StorageQuery> query = this.controller
                    .getStorageQuery();
            if (i % 2 == 0 || i < 10) {
                assertNull(query.get().singleEntryQuery("" + i, feedId,
                        this.configurator));
            } else
                assertEquals("" + i, query.get().singleEntryQuery("" + i,
                        feedId, this.configurator).getId());
            query.decrementRef();
        }

        this.controller.forceWrite();
        IndexSearcher searcher = new IndexSearcher(this.dir);

        for (int i = 1; i < this.count; i++) {
            Query luceneQuery = new TermQuery(new Term(
                    StorageEntryWrapper.FIELD_ENTRY_ID, "" + i));
            Hits hits = searcher.search(luceneQuery);
            if (i % 2 == 0 || i < 10) {

                assertEquals(0, hits.length());
            } else
                assertEquals(1, hits.length());
        }
        searcher.close();

    }

    public void testSaveUser() throws StorageException, IOException {

        GDataAccount user = new GDataAccount();
        user.setName(username);
        user.setPassword(password);
        StorageAccountWrapper wrapper = new StorageAccountWrapper(user);
        this.modifier.createAccount(wrapper);
        IndexSearcher searcher = new IndexSearcher(this.dir);
        Query q = new TermQuery(new Term(StorageAccountWrapper.FIELD_ACCOUNTNAME,
                username));
        Hits h = searcher.search(q);
        assertEquals("length == 1", 1, h.length());
        GDataAccount storedUser = StorageAccountWrapper.buildEntity(h.doc(0));
        assertTrue(storedUser.equals(user));
        searcher.close();
    }

    public void testDeleteUser() throws StorageException, IOException {
        testSaveUser();
        this.modifier.deleteAccount(username);
        IndexSearcher searcher = new IndexSearcher(this.dir);
        Query q = new TermQuery(new Term(StorageAccountWrapper.FIELD_ACCOUNTNAME,
                username));
        Hits h = searcher.search(q);
        assertEquals("length == 0", 0, h.length());
        searcher.close();
    }

    public void testUpdateUser() throws StorageException, IOException {
        testSaveUser();
        GDataAccount user = new GDataAccount();
        user.setName(username);
        user.setPassword("newPass");
        StorageAccountWrapper wrapper = new StorageAccountWrapper(user);
        this.modifier.updateAccount(wrapper);
        IndexSearcher searcher = new IndexSearcher(this.dir);
        Query q = new TermQuery(new Term(StorageAccountWrapper.FIELD_ACCOUNTNAME,
                username));
        Hits h = searcher.search(q);
        assertEquals("length == 1", 1, h.length());
        GDataAccount storedUser = StorageAccountWrapper.buildEntity(h.doc(0));
        assertTrue(storedUser.equals(user));

        assertFalse(storedUser.getPassword().equals(password));
        searcher.close();
    }

    public void testSaveFeed() throws IOException, StorageException {
        String title = "myTitle";
        ServerBaseFeed feed = new ServerBaseFeed();
        feed.setId(feedId);
        feed.setTitle(new PlainTextConstruct(title));
        feed.setServiceType(service);
        feed.setServiceConfig(this.configurator);
        StorageFeedWrapper wrapper = new StorageFeedWrapper(feed,username);
        this.modifier.createFeed(wrapper);
        
        IndexSearcher searcher = new IndexSearcher(this.dir);
        Query q = new TermQuery(new Term(StorageFeedWrapper.FIELD_FEED_ID,
                feedId));
        Hits h = searcher.search(q);
        assertEquals("length == 1", 1, h.length());
        searcher.close();
        
    }

    public void testDeleteFeed() throws IOException, StorageException {
        testSaveFeed();
        Entry e = new Entry();
        e.setTitle(new PlainTextConstruct("hello world"));
        ServerBaseEntry entry = new ServerBaseEntry(e);
        entry.setFeedId(feedId);
        entry.setId("testme");
        entry.setServiceConfig(this.configurator);
        StorageEntryWrapper entryWrapper = new StorageEntryWrapper(entry,StorageOperation.INSERT);
        this.modifier.insertEntry(entryWrapper);
        this.modifier.forceWrite();
        this.modifier.deleteFeed(feedId);
        IndexSearcher searcher = new IndexSearcher(this.dir);
        Query q = new TermQuery(new Term(StorageFeedWrapper.FIELD_FEED_ID,
                feedId));
        Query q1 = new TermQuery(new Term(StorageEntryWrapper.FIELD_FEED_REFERENCE,
                feedId));
        BooleanQuery boolQuery = new BooleanQuery();
        boolQuery.add(q,BooleanClause.Occur.SHOULD);
        boolQuery.add(q1,BooleanClause.Occur.SHOULD);
        Hits h = searcher.search(boolQuery);
        assertEquals("length == 0", 0, h.length());
        searcher.close();
        
        
        
    }

    /**
     * @throws IOException
     * @throws StorageException
     */
    public void testUpdateFeed() throws IOException, StorageException {
        testSaveFeed();
        ServerBaseFeed feed = new ServerBaseFeed();
        String title = "myTitle";
        String newusername = "doug";
        feed.setTitle(new PlainTextConstruct(title));
        feed.setId(feedId);
        feed.setServiceType(service);
        feed.setServiceConfig(this.configurator);
        StorageFeedWrapper wrapper = new StorageFeedWrapper(feed,newusername);
        this.modifier.updateFeed(wrapper);
        IndexSearcher searcher = new IndexSearcher(this.dir);
        Query q = new TermQuery(new Term(StorageFeedWrapper.FIELD_FEED_ID,
                feedId));
        Hits h = searcher.search(q);
        assertEquals("length == 1", 1, h.length());
        assertTrue(h.doc(0).get(StorageFeedWrapper.FIELD_ACCOUNTREFERENCE).equals(newusername));
        searcher.close();

    }

    private Thread getRunnerThread(int idIndex) {
        Thread t = new Thread(new Runner(idIndex));
        return t;
    }

    private class Runner implements Runnable {
        private int idIndex;

        public Runner(int idIndex) {
            this.idIndex = idIndex;
        }

        public void run() {
            for (int i = idIndex; i < idIndex + 10; i++) {

                BaseEntry e = buildEntry("" + i);
                try {
                    ServerBaseEntry en = new ServerBaseEntry(e);
                    en.setFeedId(feedId);
                    en.setServiceConfig(configurator);
                    StorageEntryWrapper wrapper = new StorageEntryWrapper(en,
                            StorageOperation.INSERT);
                    modifier.insertEntry(wrapper);
                } catch (Exception e1) {

                    e1.printStackTrace();
                }

            }

        }// end run

        private BaseEntry buildEntry(String id) {
            Entry e = new Entry();
            e.setId(id);
            e.setTitle(new PlainTextConstruct("Monty Python"));

            e.setPublished(DateTime.now());

            e.setUpdated(DateTime.now());
            String content = "1st soldier with a keen interest in birds: Who goes there?"
                    + "King Arthur: It is I, Arthur, son of Uther Pendragon, from the castle of Camelot. King of the Britons, defeater of the Saxons, Sovereign of all England!"
                    + "1st soldier with a keen interest in birds: Pull the other one!"
                    + "King Arthur: I am, and this is my trusty servant Patsy. We have ridden the length and breadth of the land in search of knights who will join me in my court at Camelot. I must speak with your lord and master."
                    + "1st soldier with a keen interest in birds: What? Ridden on a horse?"
                    + "King Arthur: Yes!";
            e.setContent(new TextContent(new PlainTextConstruct(content)));
            e.setSummary(new PlainTextConstruct("The Holy Grail"));
            return e;
        }

    }
    
    private ServerBaseEntry getServerEntry(BaseEntry e){
        ServerBaseEntry en = new ServerBaseEntry(e);
        en.setFeedId(feedId);
        en.setServiceConfig(this.configurator);
        return en;
    }

}
