package org.apache.lucene.gdata.storage.lucenestorage; 
 
import java.io.IOException; 
import java.util.ArrayList; 
import java.util.List; 
 
import junit.framework.TestCase; 
 
import org.apache.lucene.analysis.standard.StandardAnalyzer; 
import org.apache.lucene.gdata.server.FeedNotFoundException; 
import org.apache.lucene.gdata.server.registry.FeedInstanceConfigurator; 
import org.apache.lucene.gdata.server.registry.GDataServerRegistry; 
import org.apache.lucene.gdata.server.registry.RegistryBuilder; 
import org.apache.lucene.gdata.storage.StorageException; 
import org.apache.lucene.gdata.storage.lucenestorage.StorageCoreController; 
import org.apache.lucene.gdata.storage.lucenestorage.StorageEntryWrapper; 
import org.apache.lucene.gdata.storage.lucenestorage.StorageModifier; 
import org.apache.lucene.gdata.storage.lucenestorage.StorageQuery; 
import org.apache.lucene.gdata.storage.lucenestorage.StorageEntryWrapper.StorageOperation; 
import org.apache.lucene.gdata.storage.lucenestorage.util.ReferenceCounter; 
import org.apache.lucene.index.IndexWriter; 
import org.apache.lucene.store.Directory; 
import org.apache.lucene.store.RAMDirectory; 
 
import com.google.gdata.data.BaseEntry; 
import com.google.gdata.data.DateTime; 
import com.google.gdata.data.Entry; 
import com.google.gdata.data.ExtensionProfile; 
import com.google.gdata.data.Feed; 
import com.google.gdata.util.ParseException; 
 
public class TestStorageQuery extends TestCase { 
    private StorageModifier modifier; 
    private int count = 30; 
    private ReferenceCounter<StorageQuery> query; 
    private ExtensionProfile profile; 
 
    private  Directory dir; 
    private static String feedId = "myFeed"; 
    protected void setUp() throws Exception { 
        FeedInstanceConfigurator configurator = new FeedInstanceConfigurator(); 
        configurator.setFeedType(Feed.class); 
        configurator.setFeedId(feedId); 
        configurator.setExtensionProfileClass(ExtensionProfile.class); 
        GDataServerRegistry.getRegistry().registerFeed(configurator); 
        this.profile = new ExtensionProfile(); 
        this.dir = new RAMDirectory(); 
        IndexWriter writer; 
        writer = new IndexWriter(this.dir,new StandardAnalyzer(),true); 
        writer.close(); 
        this.modifier = StorageCoreController.getStorageCoreController(this.dir).getStorageModifier(); 
        insertEntries(this.count); 
        this.query = StorageCoreController.getStorageCoreController().getStorageQuery(); 
       
         
         
    } 
     
     
    public void insertEntries(int count) throws IOException,InterruptedException, StorageException{ 
        List<StorageEntryWrapper> tempList = new ArrayList<StorageEntryWrapper>(); 
        for (int i = 0; i <= count ; i++) { 
            Entry entry = new Entry(); 
            entry.setId(""+i); 
             
            entry.setUpdated(new DateTime(System.currentTimeMillis(),0)); 
            StorageEntryWrapper wrapper = new StorageEntryWrapper(entry,feedId,StorageOperation.INSERT,this.profile); 
            tempList.add(i,wrapper); 
            
            // force different timestamps --> DateTime 2006-06-05T13:37:55.724Z 
            Thread.sleep(50); 
           
        } 
        for (StorageEntryWrapper entry : tempList) { 
            this.modifier.insertEntry(entry); 
        } 
         
         
         
         
    } 
 
    protected void tearDown() throws Exception { 
        this.query.decrementRef(); 
        GDataServerRegistry.getRegistry().destroy();//TODO remove dependency here 
    } 
 
    /* 
     * Test method for 'org.apache.lucene.storage.lucenestorage.StorageQuery.feedQuery(String, int, int)' 
     */ 
    public void testFeedQuery() throws IOException, FeedNotFoundException, ParseException, StorageException { 
        FeedQueryHelper(this.query); 
        StorageCoreController.getStorageCoreController().forceWrite(); 
        ReferenceCounter<StorageQuery> queryAssureWritten = StorageCoreController.getStorageCoreController().getStorageQuery(); 
        
        assertNotSame(queryAssureWritten,this.query); 
        FeedQueryHelper(queryAssureWritten); 
        queryAssureWritten.decrementRef(); 
    } 
    private void FeedQueryHelper(ReferenceCounter<StorageQuery> currentQuery) throws IOException, FeedNotFoundException, ParseException{ 
        List<BaseEntry> entryList = currentQuery.get().getLatestFeedQuery(feedId,25,1,this.profile); 
         
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
        List<BaseEntry> entrySubList = currentQuery.get().getLatestFeedQuery(feedId,resultCount,offset,this.profile); 
         
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
    public void testSingleEntryQuery() throws FeedNotFoundException, ParseException, IOException { 
        for (int i = 1; i <= this.count; i++) { 
            BaseEntry entry = this.query.get().singleEntryQuery(""+i,feedId,this.profile); 
            assertEquals(""+i,entry.getId()); 
        } 
         
    } 
 
    /* 
     * Test method for 'org.apache.lucene.storage.lucenestorage.StorageQuery.entryQuery(List<String>, String)' 
     */ 
    public void testEntryQuery() throws FeedNotFoundException, ParseException, IOException, StorageException { 
        entryQueryHelper(this.query); 
        StorageCoreController.getStorageCoreController().forceWrite(); 
        ReferenceCounter<StorageQuery> queryAssureWritten = StorageCoreController.getStorageCoreController().getStorageQuery(); 
        
        assertNotSame(queryAssureWritten,query); 
        entryQueryHelper(queryAssureWritten); 
        queryAssureWritten.decrementRef(); 
    } 
     
     
    private void entryQueryHelper(ReferenceCounter<StorageQuery> currentQuery) throws IOException, FeedNotFoundException, ParseException{ 
         
        List<String> entryIdList = new ArrayList<String>(); 
        for (int i = 1; i <= this.count; i++) { 
           entryIdList.add(""+i); 
        } 
        List<BaseEntry> entryList = currentQuery.get().entryQuery(entryIdList,feedId,this.profile); 
        assertEquals(entryIdList.size(),entryList.size()); 
        List<String> entryIdCompare = new ArrayList<String>(); 
        for (BaseEntry entry : entryList) { 
            entryIdCompare.add(entry.getId()); 
        } 
        assertTrue(entryIdList.containsAll(entryIdCompare)); 
         
    } 
 
} 
