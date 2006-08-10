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

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.lucene.gdata.data.GDataAccount;
import org.apache.lucene.gdata.data.ServerBaseEntry;
import org.apache.lucene.gdata.data.ServerBaseFeed;
import org.apache.lucene.gdata.server.registry.ProvidedService;
import org.apache.lucene.gdata.storage.StorageException;
import org.apache.lucene.gdata.storage.lucenestorage.StorageEntryWrapper.StorageOperation;
import org.apache.lucene.gdata.utils.ProvidedServiceStub;
import org.apache.lucene.gdata.utils.ReferenceCounter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

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
        this.configurator = new ProvidedServiceStub();
        this.controller = new StorageCoreController();
        this.dir = new RAMDirectory();
        this.controller.setStorageDir(this.dir);
        this.controller.setKeepRecoveredFiles(false);
        this.controller.setOptimizeInterval(10);
        this.controller.setRecover(false);
        this.controller.setBufferSize(10);
        this.controller.setPersistFactor(10);
        this.controller.initialize();
        this.configurator = new ProvidedServiceStub();
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
        this.controller.destroy();
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
    
    public void testCheckVersionId() throws IOException, StorageException{
        this.modifier.forceWrite();
        ReferenceCounter<StorageQuery> sQuery = this.controller.getStorageQuery();
        ServerBaseEntry entry = new ServerBaseEntry(new Entry());
        entry.setId("test");
        entry.setServiceConfig(this.configurator);
        entry.setUpdated(new DateTime(System.currentTimeMillis(),0));
        entry.setFeedId(feedId);
        entry.setVersion(5);
        StorageEntryWrapper wrapper = new StorageEntryWrapper(entry,StorageOperation.INSERT);
        this.modifier.insertEntry(wrapper);
        //test in buffer
        assertTrue(sQuery.get().checkEntryVersion(entry.getId(),entry.getFeedId(),entry.getVersion()));
        assertFalse(sQuery.get().checkEntryVersion(entry.getId(),entry.getFeedId(),10000));
        assertFalse(sQuery.get().checkEntryVersion(entry.getId(),"someOtherFeed",entry.getVersion()));
        assertFalse(sQuery.get().checkEntryVersion("foobar",entry.getFeedId(),entry.getVersion()));
        
        
        this.modifier.forceWrite();
        //test in buffer after written
        assertTrue(sQuery.get().checkEntryVersion(entry.getId(),entry.getFeedId(),entry.getVersion()));
        assertFalse(sQuery.get().checkEntryVersion(entry.getId(),entry.getFeedId(),10000));
        assertFalse(sQuery.get().checkEntryVersion(entry.getId(),"someOtherFeed",entry.getVersion()));
        assertFalse(sQuery.get().checkEntryVersion("foobar",entry.getFeedId(),entry.getVersion()));
        sQuery.decrementRef();
        sQuery = this.controller.getStorageQuery();
        //test in index
        assertTrue(sQuery.get().checkEntryVersion(entry.getId(),entry.getFeedId(),entry.getVersion()));
        assertFalse(sQuery.get().checkEntryVersion(entry.getId(),entry.getFeedId(),10000));
        assertFalse(sQuery.get().checkEntryVersion("foobar",entry.getFeedId(),entry.getVersion()));
        sQuery.decrementRef();
        
        
        
        
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
            assertEquals("1",entry.getVersionId());
            entryIdCompare.add(entry.getId());
            
        }
        assertTrue(entryIdList.containsAll(entryIdCompare));
        
    }
    
    

}
