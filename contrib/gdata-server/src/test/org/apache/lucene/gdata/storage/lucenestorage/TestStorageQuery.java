package org.apache.lucene.gdata.storage.lucenestorage;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

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
import org.apache.lucene.store.Directory;

import com.google.gdata.data.BaseEntry;
import com.google.gdata.data.BaseFeed;
import com.google.gdata.data.DateTime;
import com.google.gdata.data.Entry;
import com.google.gdata.util.ParseException;

public class TestStorageQuery extends TestCase {
    private StorageModifier modifier;
    private int count = 30;
    private ReferenceCounter<StorageQuery> query;
    private ProvidedService configurator;
    private StorageCoreController controller;
    private  Directory dir;
    private static String feedId = "myFeed";
    private static String accountName = "simon";
    private static String service = ProvidedServiceStub.SERVICE_NAME;
    protected void setUp() throws Exception {
        GDataServerRegistry.getRegistry().registerComponent(StorageCoreController.class);
        this.configurator = new ProvidedServiceStub();
        
        
        GDataServerRegistry.getRegistry().registerService(this.configurator);
        this.controller = (StorageCoreController)GDataServerRegistry.getRegistry().lookup(StorageController.class,ComponentType.STORAGECONTROLLER);
        this.modifier = this.controller.getStorageModifier();
        this.dir = this.controller.getDirectory();        
        ServerBaseFeed feed = new ServerBaseFeed();
        feed.setId(feedId);
        feed.setServiceType(service);
        feed.setServiceConfig(this.configurator);
        
        StorageFeedWrapper wrapper = new StorageFeedWrapper(feed,accountName);
        this.modifier.createFeed(wrapper);
        insertEntries(this.count);
        this.query = this.controller.getStorageQuery();
        
       
        
        
    }
    
    
    /**
     * @param entrycount
     * @throws IOException
     * @throws InterruptedException
     * @throws StorageException
     */
    public void insertEntries(int entrycount) throws IOException,InterruptedException, StorageException{
        List<StorageEntryWrapper> tempList = new ArrayList<StorageEntryWrapper>();
        for (int i = 0; i <= entrycount ; i++) {
            ServerBaseEntry entry = new ServerBaseEntry(new Entry());
            entry.setId(""+i);
            entry.setServiceConfig(this.configurator);
            entry.setUpdated(new DateTime(System.currentTimeMillis(),0));
            entry.setFeedId(feedId);
            StorageEntryWrapper wrapper = new StorageEntryWrapper(entry,StorageOperation.INSERT);
            tempList.add(i,wrapper);
           
            // force different timestamps --> DateTime 2006-06-05T13:37:55.724Z
            Thread.sleep(10);
          
        }
        for (StorageEntryWrapper entry : tempList) {
            this.modifier.insertEntry(entry);
        }
        
        
        
        
    }

    protected void tearDown() throws Exception {
        this.query.decrementRef();
        GDataServerRegistry.getRegistry().destroy();
    }
    
    /*
     *  
     */
    public void testAccountNameQuery() throws IOException, StorageException{
        ReferenceCounter<StorageQuery> query = this.controller.getStorageQuery();
        assertEquals(accountName,query.get().getAccountNameForFeedId(feedId));
        assertNull(query.get().getAccountNameForFeedId("someId"));
    }

    /*
     * Test method for 'org.apache.lucene.storage.lucenestorage.StorageQuery.feedQuery(String, int, int)'
     */
    public void testFeedQuery() throws IOException,  ParseException, StorageException {
        feedQueryHelper(this.query);
        this.controller.forceWrite();
        ReferenceCounter<StorageQuery> queryAssureWritten = this.controller.getStorageQuery();
       
        assertNotSame(queryAssureWritten,this.query);
        feedQueryHelper(queryAssureWritten);
        queryAssureWritten.decrementRef();
    }
    private void feedQueryHelper(ReferenceCounter<StorageQuery> currentQuery) throws IOException,  ParseException{
       BaseFeed feed = currentQuery.get().getLatestFeedQuery(feedId,25,1,this.configurator);
       List<BaseEntry> entryList = feed.getEntries(); 
        assertTrue("listSize: "+entryList.size(),entryList.size() == 25);
        
        BaseEntry tempEntry = null;
        for (BaseEntry entry : entryList) {
          
            assertNotNull("entry",entry);
            if(tempEntry != null){
                assertTrue(tempEntry.getUpdated().compareTo(entry.getUpdated())>=0) ;
                tempEntry = entry;
            }else
                tempEntry = entry;
            
        }
        // test sub retrieve sublist
        int offset = 15;
        int resultCount = 5; 
        feed = currentQuery.get().getLatestFeedQuery(feedId,resultCount,offset,this.configurator);
        List<BaseEntry> entrySubList = feed.getEntries();
        
        assertTrue("listSize: "+entrySubList.size(),entrySubList.size() == resultCount);
        offset--;
        for (BaseEntry entry : entrySubList) {
            
            assertEquals(entry.getId(),entryList.get(offset).getId());
            offset++;
            
        }
        
        
        
    }

    /*
     * Test method for 'org.apache.lucene.storage.lucenestorage.StorageQuery.singleEntryQuery(String, String)'
     */
    public void testSingleEntryQuery() throws  ParseException, IOException {
        for (int i = 1; i <= this.count; i++) {
            BaseEntry entry = this.query.get().singleEntryQuery(""+i,feedId,this.configurator);
            assertEquals(""+i,entry.getId());
        }
        
    }

    /*
     * Test method for 'org.apache.lucene.storage.lucenestorage.StorageQuery.entryQuery(List<String>, String)'
     */
    public void testEntryQuery() throws  ParseException, IOException, StorageException {
        entryQueryHelper(this.query);
        this.controller.forceWrite();
        ReferenceCounter<StorageQuery> queryAssureWritten = this.controller.getStorageQuery();
       
        assertNotSame(queryAssureWritten,query);
        entryQueryHelper(queryAssureWritten);
        queryAssureWritten.decrementRef();
    }
    public void testGetUser() throws StorageException, IOException{
        this.modifier.forceWrite();
        GDataAccount user = new GDataAccount();
        user.setName("simon");
        user.setPassword("pass");
        user.setAuthorname("simon willnauer");
        user.setAuthorMail("simon@apache.org");
        user.setAuthorLink(new URL("http://www.apache.org"));
        
       
     
        this.modifier.createAccount(new StorageAccountWrapper(user));
        GDataAccount queriedUser = this.query.get().getUser("simon");
        assertNull(queriedUser);
        ReferenceCounter<StorageQuery> tempQuery = this.controller.getStorageQuery();
        queriedUser = tempQuery.get().getUser("simon");
        assertTrue(queriedUser.equals(user));
        assertTrue(queriedUser.getAuthorMail().equals(user.getAuthorMail()));
        assertTrue(queriedUser.getAuthorLink().equals(user.getAuthorLink()));
        assertTrue(queriedUser.getAuthorname().equals(user.getAuthorname()));
        assertTrue(queriedUser.getPassword().equals(user.getPassword()));
    }
    
    public void testIsEntryStored() throws IOException{
        
      assertTrue(this.query.get().isEntryStored(""+(this.count-1),feedId));
      assertFalse(this.query.get().isEntryStored("someOther",feedId));
      this.modifier.forceWrite();
      assertTrue(this.query.get().isEntryStored(""+(this.count-1),feedId));
      this.query = this.controller.getStorageQuery();
      assertTrue(this.query.get().isEntryStored(""+(this.count-1),feedId));
      assertFalse(this.query.get().isEntryStored("someOther",feedId));
    }
    
    public void testGetEntryLastModied() throws IOException, StorageException{
        ServerBaseEntry entry = new ServerBaseEntry(new Entry());
        entry.setId("test");
        entry.setServiceConfig(this.configurator);
        entry.setUpdated(new DateTime(System.currentTimeMillis(),0));
        entry.setFeedId(feedId);
        StorageEntryWrapper wrapper = new StorageEntryWrapper(entry,StorageOperation.INSERT);
        
        this.modifier.insertEntry(wrapper);
        assertEquals(entry.getUpdated().getValue(),this.query.get().getEntryLastModified("test",feedId));
        this.modifier.forceWrite();
        assertEquals(entry.getUpdated().getValue(),this.query.get().getEntryLastModified("test",feedId));
        this.query = this.controller.getStorageQuery();
        assertEquals(entry.getUpdated().getValue(),this.query.get().getEntryLastModified("test",feedId));
        try{
        this.query.get().getEntryLastModified("some",feedId);
        fail("exception expected");
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public void testGetFeedLastModified() throws StorageException, IOException{
        ServerBaseEntry entry = new ServerBaseEntry(new Entry());
        entry.setId("test");
        entry.setServiceConfig(this.configurator);
        entry.setUpdated(new DateTime(System.currentTimeMillis(),0));
        entry.setFeedId(feedId);
        StorageEntryWrapper wrapper = new StorageEntryWrapper(entry,StorageOperation.INSERT);
        this.modifier.insertEntry(wrapper);
        assertEquals(entry.getUpdated().getValue(),this.query.get().getFeedLastModified(feedId));
        this.modifier.forceWrite();
        assertEquals(entry.getUpdated().getValue(),this.query.get().getFeedLastModified(feedId));
        this.query = this.controller.getStorageQuery();
        assertEquals(entry.getUpdated().getValue(),this.query.get().getFeedLastModified(feedId));
    }
    private void entryQueryHelper(ReferenceCounter<StorageQuery> currentQuery) throws IOException,  ParseException{
        
        List<String> entryIdList = new ArrayList<String>();
        for (int i = 1; i <= this.count; i++) {
           entryIdList.add(""+i);
        }
        List<BaseEntry> entryList = currentQuery.get().entryQuery(entryIdList,feedId,this.configurator);
        assertEquals(entryIdList.size(),entryList.size());
        List<String> entryIdCompare = new ArrayList<String>();
        for (BaseEntry entry : entryList) {
            entryIdCompare.add(entry.getId());
        }
        assertTrue(entryIdList.containsAll(entryIdCompare));
        
    }
    
    

}
