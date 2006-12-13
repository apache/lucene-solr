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
import java.util.List;

import junit.framework.TestCase;

import org.apache.lucene.gdata.data.ServerBaseEntry;
import org.apache.lucene.gdata.storage.lucenestorage.StorageEntryWrapper.StorageOperation;
import org.apache.lucene.gdata.utils.ProvidedServiceStub;

import com.google.gdata.data.DateTime;

public class TestStorageBuffer extends TestCase {
    private static final String FEEDID = "feed";
    private static final String ENTRYID = "someID";
    private StorageBuffer buffer;
    protected void setUp() throws Exception {
        super.setUp();
        this.buffer = new StorageBuffer(10);
    }

    protected void tearDown() throws Exception {
        this.buffer.close();
    }

    /*
     * Test method for 'org.apache.lucene.gdata.storage.lucenestorage.StorageBuffer.StorageBuffer(int)'
     */
    public void testStorageBuffer() {
        assertEquals(StorageBuffer.DEFAULT_BUFFER_COUNT,new StorageBuffer(StorageBuffer.DEFAULT_BUFFER_COUNT-1).getBufferSize());
        assertEquals(StorageBuffer.DEFAULT_BUFFER_COUNT,new StorageBuffer(StorageBuffer.DEFAULT_BUFFER_COUNT).getBufferSize());
        assertEquals(StorageBuffer.DEFAULT_BUFFER_COUNT+1,new StorageBuffer(StorageBuffer.DEFAULT_BUFFER_COUNT+1).getBufferSize());
    }

    /*
     * Test method for 'org.apache.lucene.gdata.storage.lucenestorage.StorageBuffer.addEntry(StorageEntryWrapper)'
     */
    public void testAddEntry() throws IOException {
        
        ServerBaseEntry e = createServerBaseEntry(ENTRYID,FEEDID);
        StorageEntryWrapper wrapper = new StorageEntryWrapper(e,StorageOperation.INSERT);
        this.buffer.addEntry(wrapper);
        assertEquals(1,this.buffer.getSortedEntries(FEEDID).size());
        this.buffer.addEntry(wrapper);
        assertEquals(1,this.buffer.getSortedEntries(FEEDID).size());
        
        e.setId("someotherID");
        e.setFeedId(FEEDID);
        e.setServiceConfig(new ProvidedServiceStub());
        wrapper = new StorageEntryWrapper(e,StorageOperation.INSERT);
        this.buffer.addEntry(wrapper);
        assertEquals(2,this.buffer.getSortedEntries(FEEDID).size());
        e.setId("someotherID");
        e.setFeedId("someOtherFeed");
        e.setServiceConfig(new ProvidedServiceStub());
        wrapper = new StorageEntryWrapper(e,StorageOperation.UPDATE);
        this.buffer.addEntry(wrapper);
        wrapper = new StorageEntryWrapper(e,StorageOperation.DELETE);
        e.setId("deleted and ingnored");
        e.setFeedId("someOtherFeed");
        e.setServiceConfig(new ProvidedServiceStub());
        this.buffer.addEntry(wrapper);
        assertEquals(2,this.buffer.getSortedEntries(FEEDID).size());
        assertEquals(1,this.buffer.getSortedEntries("someOtherFeed").size());
        assertEquals("Contains 2 different IDs",2,this.buffer.getExculdList().length);
        
    }
    /*
     * Test method for 'org.apache.lucene.gdata.storage.lucenestorage.StorageBuffer.addDeleted(String, String)'
     */
    public void testAddDeleted() throws IOException {
        
        this.buffer.addDeleted(ENTRYID,FEEDID);
        assertNull(this.buffer.getSortedEntries(FEEDID));
        assertEquals(1,this.buffer.getExculdList().length);
        assertEquals(ENTRYID,this.buffer.getExculdList()[0]);
        
        this.buffer.addDeleted(ENTRYID,FEEDID);
        assertNull(this.buffer.getSortedEntries(FEEDID));
        assertEquals(1,this.buffer.getExculdList().length);
        assertEquals(ENTRYID,this.buffer.getExculdList()[0]);
        
        
    }
    /*
     * Test method for 'org.apache.lucene.gdata.storage.lucenestorage.StorageBuffer.getFeedLastModified(String)'
     */
    public void testGetFeedLastModified() throws IOException, InterruptedException {
        ServerBaseEntry e = createServerBaseEntry(ENTRYID,FEEDID);
        e.setUpdated(new DateTime(System.currentTimeMillis()-200,0));
        StorageEntryWrapper wrapper = new StorageEntryWrapper(e,StorageOperation.INSERT);
        this.buffer.addEntry(wrapper);
        assertEquals(new Long(e.getUpdated().getValue()),this.buffer.getFeedLastModified(FEEDID));
        //test update
//      force timestamp
        e.setUpdated(new DateTime(System.currentTimeMillis()-180,0));
        wrapper = new StorageEntryWrapper(e,StorageOperation.UPDATE);
        this.buffer.addEntry(wrapper);
        Long firstAddTimestamp = new Long(e.getUpdated().getValue());
        assertEquals(firstAddTimestamp,this.buffer.getFeedLastModified(FEEDID));
//      force timestamp
        e.setUpdated(new DateTime(System.currentTimeMillis()-160,0));
        assertFalse("updated after add" ,e.getUpdated().equals(this.buffer.getFeedLastModified(FEEDID)));
        
        //insert for other feed
        String otherID = "someOtherFeedID";
        e.setFeedId(otherID);
//      force timestamp
        e.setUpdated(new DateTime(System.currentTimeMillis()-140,0));
        wrapper = new StorageEntryWrapper(e,StorageOperation.INSERT);
        this.buffer.addEntry(wrapper);
        assertEquals(new Long(e.getUpdated().getValue()),this.buffer.getFeedLastModified(otherID));
        assertEquals(firstAddTimestamp,this.buffer.getFeedLastModified(FEEDID));
        
        assertTrue(firstAddTimestamp.equals(this.buffer.getFeedLastModified(FEEDID)));
        this.buffer.addDeleted(e.getId(),FEEDID);
        // time will be set inside the buffer
        assertTrue(firstAddTimestamp < this.buffer.getFeedLastModified(FEEDID));
        
        
    }
    
  
    /*
     * Test method for 'org.apache.lucene.gdata.storage.lucenestorage.StorageBuffer.getSortedEntries(String)'
     */
    public void testGetSortedEntries() throws IOException, InterruptedException {
        assertNull(this.buffer.getSortedEntries(FEEDID));
        ServerBaseEntry e = createServerBaseEntry("2",FEEDID);
        e.setUpdated(new DateTime(System.currentTimeMillis()-200,0));
        StorageEntryWrapper wrapper = new StorageEntryWrapper(e,StorageOperation.INSERT);
        this.buffer.addEntry(wrapper);
        e.setId("0");
//      force timestamp
        e.setUpdated(new DateTime(System.currentTimeMillis()-180,0));
        wrapper = new StorageEntryWrapper(e,StorageOperation.INSERT);
        this.buffer.addEntry(wrapper);
        e.setId("1");
//      force timestamp
        e.setUpdated(new DateTime(System.currentTimeMillis()-160,0));
        wrapper = new StorageEntryWrapper(e,StorageOperation.INSERT);
        this.buffer.addEntry(wrapper);
        e.setId("0");
//      force timestamp
        e.setUpdated(new DateTime(System.currentTimeMillis()-140,0));
        wrapper = new StorageEntryWrapper(e,StorageOperation.UPDATE);
        this.buffer.addEntry(wrapper);
//      force timestamp
        e.setUpdated(new DateTime(System.currentTimeMillis()-120,0));
        wrapper = new StorageEntryWrapper(e,StorageOperation.DELETE);
        this.buffer.addEntry(wrapper);
        List<StorageEntryWrapper> list = this.buffer.getSortedEntries(FEEDID);
       assertEquals(3,list.size());
        for (int i = 0; i < 3; i++) {
            assertEquals(""+i,list.get(i).getEntryId());   
        }
        

    }

  

    /*
     * Test method for 'org.apache.lucene.gdata.storage.lucenestorage.StorageBuffer.getEntry(String, String)'
     */
    public void testGetEntry() throws IOException {
        assertNull(this.buffer.getEntry(ENTRYID,FEEDID));
        ServerBaseEntry e = createServerBaseEntry(ENTRYID,FEEDID);
        StorageEntryWrapper wrapper = new StorageEntryWrapper(e,StorageOperation.INSERT);
        this.buffer.addEntry(wrapper);
        assertSame(wrapper,this.buffer.getEntry(ENTRYID,FEEDID));
        
        e = createServerBaseEntry("0",FEEDID);
        wrapper = new StorageEntryWrapper(e,StorageOperation.UPDATE);
        this.buffer.addEntry(wrapper);
        assertSame(wrapper,this.buffer.getEntry("0",FEEDID));
        
        
    }

    /*
     * Test method for 'org.apache.lucene.gdata.storage.lucenestorage.StorageBuffer.getExculdList()'
     */
    public void testGetExculdList() throws IOException {
        ServerBaseEntry e = createServerBaseEntry(ENTRYID,FEEDID);
        StorageEntryWrapper wrapper = new StorageEntryWrapper(e,StorageOperation.INSERT);
        this.buffer.addEntry(wrapper);
        this.buffer.addEntry(wrapper);
        assertEquals(1,this.buffer.getExculdList().length);
        assertEquals(wrapper.getEntryId(),this.buffer.getExculdList()[0]);
        
        wrapper = new StorageEntryWrapper(e,StorageOperation.UPDATE);
        this.buffer.addEntry(wrapper);
        assertEquals(1,this.buffer.getExculdList().length);
        assertEquals(wrapper.getEntryId(),this.buffer.getExculdList()[0]);
        this.buffer.addDeleted(ENTRYID,FEEDID);
        assertEquals(1,this.buffer.getExculdList().length);
        assertEquals(wrapper.getEntryId(),this.buffer.getExculdList()[0]);
        
         e = createServerBaseEntry("someOtherEntry","someOtherFeed");
        wrapper = new StorageEntryWrapper(e,StorageOperation.INSERT);
        this.buffer.addEntry(wrapper);
        this.buffer.addEntry(wrapper);
        assertEquals(2,this.buffer.getExculdList().length);
        
        
    }

    /*
     * Test method for 'org.apache.lucene.gdata.storage.lucenestorage.StorageBuffer.close()'
     */
    public void testClose() throws IOException {
        ServerBaseEntry e = createServerBaseEntry(ENTRYID,FEEDID);
        StorageEntryWrapper wrapper = new StorageEntryWrapper(e,StorageOperation.INSERT);
        
        this.buffer.addEntry(wrapper);
        assertNotNull(this.buffer.getSortedEntries(FEEDID));
        assertNotNull(this.buffer.getEntry(ENTRYID,FEEDID));
        assertEquals(1,this.buffer.getExculdList().length);
        this.buffer.close();
        assertNull(this.buffer.getSortedEntries(FEEDID));
        assertNull(this.buffer.getEntry(ENTRYID,FEEDID));
        assertEquals(0,this.buffer.getExculdList().length);
        
        

    }
    public ServerBaseEntry createServerBaseEntry(String entryID, String feedId) throws IOException{
        ServerBaseEntry e = new ServerBaseEntry();
        e.setId(entryID);
        e.setFeedId(feedId);
        e.setServiceConfig(new ProvidedServiceStub());
       return e;
    }
}
