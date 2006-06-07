package org.apache.lucene.gdata.storage.lucenestorage; 
 
import java.io.IOException; 
import java.util.Date; 
 
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
import org.apache.lucene.index.Term; 
import org.apache.lucene.search.Hits; 
import org.apache.lucene.search.IndexSearcher; 
import org.apache.lucene.search.Query; 
import org.apache.lucene.search.TermQuery; 
import org.apache.lucene.store.Directory; 
import org.apache.lucene.store.RAMDirectory; 
 
import com.google.gdata.data.BaseEntry; 
import com.google.gdata.data.DateTime; 
import com.google.gdata.data.Entry; 
import com.google.gdata.data.ExtensionProfile; 
import com.google.gdata.data.Feed; 
import com.google.gdata.data.PlainTextConstruct; 
import com.google.gdata.data.TextConstruct; 
import com.google.gdata.data.TextContent; 
import com.google.gdata.util.ParseException; 
 
public class TestStorageModifier extends TestCase { 
    private StorageModifier modifier; 
    private int count = 1; 
    
    private ExtensionProfile profile; 
    private  Directory dir; 
 
    
    private static String feedId = "myFeed"; 
   
    protected void setUp() throws Exception { 
        FeedInstanceConfigurator configurator = new FeedInstanceConfigurator(); 
        configurator.setFeedType(Feed.class); 
        configurator.setFeedId(feedId); 
        configurator.setExtensionProfileClass(ExtensionProfile.class); 
        GDataServerRegistry.getRegistry().registerFeed(configurator); 
        dir = new RAMDirectory(); 
        this.profile = new ExtensionProfile(); 
        IndexWriter writer; 
         
        writer = new IndexWriter(dir,new StandardAnalyzer(),true); 
        writer.close(); 
        modifier = StorageCoreController.getStorageCoreController(dir).getStorageModifier(); 
         
        
    } 
 
    protected void tearDown() throws Exception { 
        this.count = 1; 
        // destroy all resources 
        GDataServerRegistry.getRegistry().destroy();//TODO remove dependency here 
         
    } 
 
    /* 
     * Test method for 
     * 'org.apache.lucene.storage.lucenestorage.StorageModifier.StorageModifier(Directory, 
     * int)' 
     */ 
    public void testStorageModifier() { 
 
    } 
 
    /* 
     * Test method for 
     * 'org.apache.lucene.storage.lucenestorage.StorageModifier.updateEntry(StroageEntryWrapper)' 
     */ 
    public void testUpdateEntry() throws IOException, InterruptedException, FeedNotFoundException, ParseException, StorageException { 
        testInsertEntry(); 
        for(int i = 1; i < this.count; i++){ 
            Entry e = new Entry(); 
            e.setId(""+i); 
            String insertString = "Hello world"+i; 
            e.setTitle(new PlainTextConstruct(insertString)); 
            StorageEntryWrapper wrapper = new StorageEntryWrapper(e,feedId,StorageOperation.UPDATE,this.profile); 
            this.modifier.updateEntry(wrapper); 
            ReferenceCounter<StorageQuery> innerQuery = StorageCoreController.getStorageCoreController().getStorageQuery(); 
            BaseEntry fetchedEntry = innerQuery.get().singleEntryQuery(""+i,feedId,this.profile); 
            assertEquals("updated Title:",insertString,fetchedEntry.getTitle().getPlainText());             
        } 
        // double updates 
        for(int i = 1; i < this.count; i++){ 
            Entry e = new Entry(); 
            e.setId(""+i); 
            String insertString = "Hello world"+i; 
            e.setTitle(new PlainTextConstruct(insertString)); 
            StorageEntryWrapper wrapper = new StorageEntryWrapper(e,feedId,StorageOperation.UPDATE,this.profile); 
            this.modifier.updateEntry(wrapper); 
             
            e = new Entry(); 
            e.setId(""+i); 
            insertString = "Foo Bar"+i; 
            e.setTitle(new PlainTextConstruct(insertString)); 
            wrapper = new StorageEntryWrapper(e,feedId,StorageOperation.UPDATE,this.profile); 
            this.modifier.updateEntry(wrapper); 
             
            ReferenceCounter<StorageQuery> innerQuery = StorageCoreController.getStorageCoreController().getStorageQuery(); 
             
            BaseEntry fetchedEntry = innerQuery.get().singleEntryQuery(""+i,feedId,this.profile); 
            assertEquals("updated Title:",insertString,fetchedEntry.getTitle().getPlainText());             
        } 
 
         
         
    } 
 
    /* 
     * Test method for 
     * 'org.apache.lucene.storage.lucenestorage.StorageModifier.insertEntry(StroageEntryWrapper)' 
     */ 
    public void testInsertEntry() throws IOException, InterruptedException, FeedNotFoundException, ParseException, StorageException { 
        
        Thread a = getRunnerThread(this.count); 
        a.start(); 
         
        Thread b = getRunnerThread((this.count+=10)); 
        b.start(); 
        a.join(); 
        for (int i = 1; i < this.count ; i++) { 
            ReferenceCounter<StorageQuery> innerQuery = StorageCoreController.getStorageCoreController().getStorageQuery(); 
            BaseEntry e = innerQuery.get().singleEntryQuery(""+i,feedId,this.profile); 
            assertEquals("get entry for id"+i,""+i,e.getId()); 
             
             
        } 
        b.join(); 
        ReferenceCounter<StorageQuery> query = StorageCoreController.getStorageCoreController().getStorageQuery(); 
         
        this.count+=10; 
        for (int i = 1; i < this.count ; i++) { 
            BaseEntry e = query.get().singleEntryQuery(""+i,feedId,this.profile); 
            assertEquals("get entry for id"+i,""+i,e.getId()); 
        } 
        
        BaseEntry e = query.get().singleEntryQuery(""+this.count,feedId,this.profile); 
        assertNull("not entry for ID",e); 
        query.decrementRef(); 
         
    } 
 
    /* 
     * Test method for 
     * 'org.apache.lucene.storage.lucenestorage.StorageModifier.deleteEntry(String)' 
     */ 
    public void testDeleteEntry() throws IOException, InterruptedException, FeedNotFoundException, ParseException, StorageException { 
        testInsertEntry(); 
        for (int i = 1; i < this.count ; i++) { 
            if(i%2 == 0 || i< 10){ 
                this.modifier.deleteEntry(""+i,feedId); 
            } 
            ReferenceCounter<StorageQuery> query = StorageCoreController.getStorageCoreController().getStorageQuery(); 
            if(i%2 == 0 || i< 10){ 
                assertNull(query.get().singleEntryQuery(""+i,feedId,this.profile)); 
            } 
            else 
                assertEquals(""+i,query.get().singleEntryQuery(""+i,feedId,this.profile).getId()); 
            query.decrementRef(); 
        } 
         
        StorageCoreController.getStorageCoreController().forceWrite(); 
        IndexSearcher searcher = new IndexSearcher(this.dir); 
         
        for (int i = 1; i < this.count ; i++) { 
            Query luceneQuery = new TermQuery(new Term(StorageEntryWrapper.FIELD_ENTRY_ID,""+i)); 
            Hits hits = searcher.search(luceneQuery); 
            if(i%2 == 0 || i< 10){ 
                 
                assertEquals(0,hits.length()); 
            } 
            else 
                assertEquals(1,hits.length()); 
        } 
        searcher.close(); 
         
    } 
     
     
    private Thread getRunnerThread(int idIndex){ 
        Thread t = new Thread(new Runner(idIndex)); 
        return t; 
    } 
     
    private class Runner implements Runnable{ 
        private int idIndex; 
        public Runner(int idIndex){ 
            this.idIndex = idIndex; 
        } 
        public void run() { 
            for (int i = idIndex; i < idIndex+10; i++) { 
               
                BaseEntry e = buildEntry(""+i); 
                try { 
                StorageEntryWrapper wrapper = new StorageEntryWrapper(e,feedId,StorageOperation.INSERT,new ExtensionProfile()); 
                modifier.insertEntry(wrapper); 
                } catch (Exception e1) { 
                     
                    e1.printStackTrace(); 
                } 
            
                 
                 
     
            } 
          
                             
        }//end run 
         
        private BaseEntry buildEntry(String id){ 
            Entry e = new Entry(); 
            e.setId(id); 
            e.setTitle(new PlainTextConstruct("Monty Python")); 
             
            e.setPublished(DateTime.now()); 
             
            e.setUpdated(DateTime.now()); 
            String content = "1st soldier with a keen interest in birds: Who goes there?" + 
                    "King Arthur: It is I, Arthur, son of Uther Pendragon, from the castle of Camelot. King of the Britons, defeater of the Saxons, Sovereign of all England!" + 
                    "1st soldier with a keen interest in birds: Pull the other one!" + 
                    "King Arthur: I am, and this is my trusty servant Patsy. We have ridden the length and breadth of the land in search of knights who will join me in my court at Camelot. I must speak with your lord and master." + 
                    "1st soldier with a keen interest in birds: What? Ridden on a horse?" + 
                    "King Arthur: Yes!"; 
            e.setContent(new TextContent(new PlainTextConstruct(content))); 
            e.setSummary(new PlainTextConstruct("The Holy Grail")); 
            return e; 
        } 
         
    } 
 
} 
