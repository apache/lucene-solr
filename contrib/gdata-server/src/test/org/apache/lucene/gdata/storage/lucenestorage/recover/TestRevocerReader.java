package org.apache.lucene.gdata.storage.lucenestorage.recover;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;

import junit.framework.TestCase;

import org.apache.lucene.gdata.server.registry.GDataServerRegistry;
import org.apache.lucene.gdata.server.registry.ProvidedService;
import org.apache.lucene.gdata.storage.lucenestorage.StorageEntryWrapper;
import org.apache.lucene.gdata.storage.lucenestorage.StorageEntryWrapper.StorageOperation;
import org.apache.lucene.gdata.utils.ProvidedServiceStub;

import com.google.gdata.data.DateTime;

public class TestRevocerReader extends TestCase {
    private RecoverReader recReader;
    private static final String feedId = "myFeed";
    private static final String entryId = "myID";
    private static final Long timestamp = System.currentTimeMillis();
    private String title = "myTitle";
    private static final DateTime dateTime = DateTime.now();
    private String delete = "D;"+feedId+";"+entryId+";"+timestamp+";\n###########\n";
    private String insert = "I;"+feedId+";"+entryId+";"+timestamp+";" +ProvidedServiceStub.SERVICE_NAME+";"+RecoverWriter.META_DATA_ENTRY_SEPARATOR+
    "<atom:entry xmlns:atom='http://www.w3.org/2005/Atom'><atom:id>"+entryId+"</atom:id><atom:updated>"+dateTime.toString()+"</atom:updated><atom:title type='text'>" + this.title+
    "</atom:title></atom:entry>"+RecoverWriter.META_DATA_ENTRY_SEPARATOR+RecoverWriter.STORAGE_OPERATION_SEPARATOR+RecoverWriter.META_DATA_ENTRY_SEPARATOR;
    protected void setUp() throws Exception {
        this.recReader = new RecoverReader();
        GDataServerRegistry.getRegistry().registerService(new ProvidedServiceStub());
        
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    /*
     * Test method for 'org.apache.lucene.gdata.storage.lucenestorage.recover.RecoverReader.getNonDeleteEntries(Reader)'
     */
    public void testRecoverDeletedEntries() throws IOException {
        StringReader reader = new StringReader(this.delete);
        assertNotNull(this.recReader.recoverEntries(new BufferedReader(reader)));
        reader = new StringReader(this.delete);
        List<StorageEntryWrapper> recList = this.recReader.recoverEntries(new BufferedReader(reader));
        assertEquals(1,recList.size());
        StorageEntryWrapper delWrapper = recList.get(0);
        assertEquals(StorageOperation.DELETE,delWrapper.getOperation());
        assertEquals(feedId,delWrapper.getFeedId());
        assertEquals(entryId,delWrapper.getEntryId());
        
    }
    public void testRecoverInsertedEntries() throws IOException {
        
        StringReader reader = new StringReader(this.insert);
        List<StorageEntryWrapper> recList = this.recReader.recoverEntries(new BufferedReader(reader));
        assertEquals(1,recList.size());
        StorageEntryWrapper insWrapper = recList.get(0);
        assertEquals(StorageOperation.INSERT,insWrapper.getOperation());
        assertEquals(feedId,insWrapper.getFeedId());
        assertEquals(entryId,insWrapper.getEntryId());
        assertEquals(dateTime,insWrapper.getEntry().getUpdated());
        assertEquals(this.title,insWrapper.getEntry().getTitle().getPlainText());
        
        
    }
    
    public void testRecoverReader()throws IOException{
        StringReader reader = new StringReader(this.insert+this.delete);
        List<StorageEntryWrapper> recList = this.recReader.recoverEntries(new BufferedReader(reader));
        assertEquals(2,recList.size());
        assertEquals(StorageOperation.INSERT,recList.get(0).getOperation());
        assertEquals(StorageOperation.DELETE,recList.get(1).getOperation());
        
        reader = new StringReader("some corrupted\n###########\n"+this.insert);
        recList = this.recReader.recoverEntries(new BufferedReader(reader));
        assertEquals(1,recList.size());
        assertEquals(StorageOperation.INSERT,recList.get(0).getOperation());
        
    }
}
