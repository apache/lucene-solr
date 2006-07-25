package org.apache.lucene.gdata.storage.lucenestorage.recover;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.lucene.gdata.data.ServerBaseEntry;
import org.apache.lucene.gdata.server.registry.GDataServerRegistry;
import org.apache.lucene.gdata.storage.StorageException;
import org.apache.lucene.gdata.storage.lucenestorage.StorageEntryWrapper;
import org.apache.lucene.gdata.storage.lucenestorage.StorageModifierStub;
import org.apache.lucene.gdata.storage.lucenestorage.StorageEntryWrapper.StorageOperation;
import org.apache.lucene.gdata.utils.ProvidedServiceStub;

import com.google.gdata.data.DateTime;

public class TestRecoverController extends TestCase {
    private RecoverController writeController;
    private RecoverController readController;
    private File recDir;
    private String feedId = "feedid";
    private String entryId = "entryId";
    
    protected void setUp() throws Exception {
        this.recDir = new File("unittest"+System.currentTimeMillis());
        if(!this.recDir.exists())
            this.recDir.mkdir();
        this.recDir.deleteOnExit();
        GDataServerRegistry.getRegistry().registerService(new ProvidedServiceStub());
        this.writeController = new RecoverController(this.recDir,false,true);
        this.readController = new RecoverController(this.recDir,true,true);
        
        

        
    }

    protected void tearDown() throws Exception {
        
        
        
        File[] files = this.recDir.listFiles();
        for (int i = 0; i < files.length; i++) {
            files[i].delete();
        }
        this.recDir.delete();
      GDataServerRegistry.getRegistry().destroy();
    }

    /*
     * Test method for 'org.apache.lucene.gdata.storage.lucenestorage.recover.RecoverController.storageModified(StorageEntryWrapper)'
     */
    public void testStorageModified() throws IOException, RecoverException {
        this.writeController.initialize();
        ServerBaseEntry entry = new ServerBaseEntry();
        entry.setFeedId(this.feedId);
        entry.setId(entryId);
        entry.setUpdated(DateTime.now());
        entry.setServiceConfig(new ProvidedServiceStub());
        StorageEntryWrapper wrapper = new StorageEntryWrapper(entry,StorageOperation.INSERT);
        this.writeController.storageModified(wrapper);
        assertEquals(1,this.recDir.listFiles().length);
        this.writeController.destroy();
    }

    /*
     * Test method for 'org.apache.lucene.gdata.storage.lucenestorage.recover.RecoverController.recoverEntries(StorageModifier)'
     */
    public void testRecoverEntries() throws IOException, StorageException, RecoverException {
        testStorageModified();
        
        int length = this.recDir.listFiles().length;
        assertEquals(1,length);
        
        this.readController.initialize();
        try{
        this.readController.recoverEntries(new StorageModifierStub(null,null,null,0,0));
        }catch (Exception e) {
            fail("unexpected exception"+e.getMessage());
        }
        this.readController.destroy();
        assertEquals(1,this.recDir.listFiles().length);
        
        createCorruptedFile();
        this.readController.initialize();
        try{
        this.readController.recoverEntries(new StorageModifierStub(null,null,null,0,0));
        }catch (Exception e) {
            fail("unexpected exception"+e.getMessage());
        }
        this.readController.destroy();
        assertEquals(2,this.recDir.listFiles().length);
    }
    
    
    private void createCorruptedFile() throws IOException{
        FileWriter writer = new FileWriter(new File(this.recDir,"somefile.rec"));
        writer.write("someString\nSomeOtherString");
        writer.close();
    }

}
