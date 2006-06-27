package org.apache.lucene.gdata.storage.lucenestorage.recover;

import java.io.IOException;
import java.io.StringWriter;

import junit.framework.TestCase;

import org.apache.lucene.gdata.data.ServerBaseEntry;
import org.apache.lucene.gdata.server.registry.ProvidedService;
import org.apache.lucene.gdata.storage.lucenestorage.StorageEntryWrapper;
import org.apache.lucene.gdata.storage.lucenestorage.StorageEntryWrapper.StorageOperation;
import org.apache.lucene.gdata.utils.ProvidedServiceStub;

import com.google.gdata.data.DateTime;
import com.google.gdata.data.Entry;
import com.google.gdata.data.PlainTextConstruct;

public class TestRecoverWriter extends TestCase {
    private static final String ENTRYID = "myID";
    private static final String TITLE = "title";
    private static final String FEEDID = "myFeed";
    
    private static final Long TIMESTAMP = 132326657L;
    private DateTime dateTime = new DateTime(TIMESTAMP,0);
    String compareEntry = "I;"+FEEDID+";"+ENTRYID+";"+TIMESTAMP+";" +ProvidedServiceStub.SERVICE_NAME+";"+RecoverWriter.META_DATA_ENTRY_SEPARATOR+
            "<atom:entry xmlns:atom='http://www.w3.org/2005/Atom'><atom:id>"+ENTRYID+"</atom:id><atom:updated>"+this.dateTime.toString()+"</atom:updated><atom:title type='text'>" + TITLE+
            "</atom:title></atom:entry>"+RecoverWriter.META_DATA_ENTRY_SEPARATOR+RecoverWriter.STORAGE_OPERATION_SEPARATOR+RecoverWriter.META_DATA_ENTRY_SEPARATOR;
    String compareDelete = "D;"+FEEDID+";"+ENTRYID+";"+TIMESTAMP+";"+RecoverWriter.META_DATA_ENTRY_SEPARATOR+RecoverWriter.STORAGE_OPERATION_SEPARATOR+RecoverWriter.META_DATA_ENTRY_SEPARATOR;
    StorageEntryWrapper wrapper;
    StorageEntryWrapper deleteWrapper;

    protected void setUp() throws Exception {
        ServerBaseEntry entry = new ServerBaseEntry(new Entry());
        entry.setId(ENTRYID);
        
        entry.setUpdated(new DateTime(TIMESTAMP,0));
        entry.setTitle(new PlainTextConstruct(TITLE));
        ProvidedService config = new ProvidedServiceStub();
        entry.setFeedId(FEEDID);
        entry.setServiceConfig(config);
        this.wrapper = new StorageEntryWrapper(entry,StorageOperation.INSERT);
        this.deleteWrapper = new StorageEntryWrapper(entry,StorageOperation.DELETE);
        
        
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        
    }

    /*
     * Test method for 'org.apache.lucene.gdata.storage.lucenestorage.recover.RecoverWriter.writeEntry(StorageEntryWrapper, Writer)'
     */
    public void testWriteEntry() throws IOException {
        RecoverWriter wr = new RecoverWriter();
        StringWriter writer = new StringWriter();
        wr.writeEntry(this.wrapper,writer);
        assertEquals(compareEntry,writer.toString());
        writer.close();
    }

    /*
     * Test method for 'org.apache.lucene.gdata.storage.lucenestorage.recover.RecoverWriter.writeDelete(String, String, Writer)'
     */
    public void testWriteDelete() throws IOException {
        RecoverWriter wr = new RecoverWriter();
        StringWriter writer = new StringWriter();
        wr.writeEntry(this.deleteWrapper,writer);
        System.out.println(writer.toString());
        assertEquals(compareDelete,writer.toString());
        writer.close();
    }

}
