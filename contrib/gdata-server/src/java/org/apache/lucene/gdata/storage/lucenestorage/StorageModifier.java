package org.apache.lucene.gdata.storage.lucenestorage; 
 
import java.io.IOException; 
import java.util.Collection; 
import java.util.HashMap; 
import java.util.LinkedList; 
import java.util.List; 
import java.util.Map; 
import java.util.concurrent.locks.Lock; 
import java.util.concurrent.locks.ReentrantReadWriteLock; 
 
import org.apache.commons.logging.Log; 
import org.apache.commons.logging.LogFactory; 
import org.apache.lucene.document.Document; 
import org.apache.lucene.gdata.storage.StorageException; 
import org.apache.lucene.index.IndexModifier; 
import org.apache.lucene.index.Term; 
 
/** 
 * TODO document this 
 * @author Simon Willnauer 
 * 
 */ 
public class StorageModifier { 
    protected static final Log LOG = LogFactory.getLog(StorageModifier.class); 
 
    private final List<Term> deletedDocumentQueue; 
    private final List<Term> deletedForUpdateDocumentQueue; 
 
    private final Map<String,Document> documentMap; 
     
 
    private volatile int persistFactor; 
 
    private volatile int modifiedCounter = 0; 
 
    private static int DEFAULT_PERSIST_FACTOR = 10; 
 
    private StorageBuffer buffer; 
 
    private IndexModifier modifier; 
 
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock(false); 
 
    private Lock readLock = this.lock.readLock(); 
 
    private Lock writeLock = this.lock.writeLock(); 
    private final static int DEFAULT_OPTIMIZE_INTERVAL = 10; 
    private final int optimizeInterval; 
    private int optimizeCounter = 0; 
 
    /** 
     * TODO document this 
     * @param modifier 
     * @param buffer 
     * @param persitsFactor 
     * @param optimizeInterval  
     */ 
    public StorageModifier(final IndexModifier modifier, 
            final StorageBuffer buffer, int persitsFactor,int optimizeInterval) { 
        this.deletedDocumentQueue = new LinkedList<Term>(); 
        this.deletedForUpdateDocumentQueue = new LinkedList<Term>(); 
        this.documentMap = new HashMap<String,Document>(persitsFactor); 
        this.buffer = buffer; 
 
        this.persistFactor = persitsFactor > 0 ? persitsFactor 
                : DEFAULT_PERSIST_FACTOR; 
        this.modifier = modifier; 
        this.optimizeInterval = optimizeInterval < DEFAULT_OPTIMIZE_INTERVAL?DEFAULT_OPTIMIZE_INTERVAL:optimizeInterval; 
 
    } 
 
    /** 
     * TODO document this 
     * @param wrapper 
     * @throws StorageException 
     */ 
    public void updateEntry(StorageEntryWrapper wrapper) 
            throws  StorageException { 
        try { 
            this.readLock.lock(); 
            Term tempTerm = new Term(StorageEntryWrapper.FIELD_ENTRY_ID, wrapper.getEntryId()); 
            this.buffer.addEntry(wrapper); 
            this.deletedForUpdateDocumentQueue.add(tempTerm); 
            this.documentMap.put(wrapper.getEntryId(),wrapper.getLuceneDocument()); 
            storageModified(); 
        } finally { 
            this.readLock.unlock(); 
        } 
    } 
 
    /** 
     * TODO document this 
     * @param wrapper 
     * @throws StorageException  
     */ 
    public void insertEntry(StorageEntryWrapper wrapper) throws StorageException { 
        this.readLock.lock(); 
        try { 
 
            this.buffer.addEntry(wrapper); 
            this.documentMap.put(wrapper.getEntryId(),wrapper.getLuceneDocument()); 
            storageModified(); 
        } finally { 
            this.readLock.unlock(); 
        } 
    } 
 
    /** 
     *TODO document this 
     * @param entryId 
     * @param feedId 
     * @throws StorageException  
     * 
     */ 
    public void deleteEntry(final String entryId, final String feedId) throws StorageException { 
        try { 
            this.readLock.lock(); 
            Term tempTerm = new Term(StorageEntryWrapper.FIELD_ENTRY_ID, entryId); 
            this.buffer.addDeleted(entryId, feedId); 
            this.deletedDocumentQueue.add(tempTerm); 
            storageModified(); 
        } finally { 
            this.readLock.unlock(); 
        } 
    } 
 
    private void storageModified() throws StorageException { 
        this.readLock.unlock(); 
        this.writeLock.lock(); 
 
        try { 
            incrementCounter(); 
            if (this.persistFactor > this.modifiedCounter) 
                return; 
 
            if (LOG.isInfoEnabled()) 
                LOG.info("Storage modified for " + this.modifiedCounter 
                        + " times. Write Persistent index"); 
            writePersistentIndex((this.optimizeCounter >= this.optimizeInterval)); 
            requestNewIndexModifier(); 
 
            this.modifiedCounter = 0; 
        } catch (IOException e) { 
            LOG.error("Writing persistent index failed - Recovering", e); 
        } finally { 
            this.readLock.lock(); 
            this.writeLock.unlock(); 
        } 
 
    } 
 
    protected void forceWrite() throws IOException, StorageException { 
        this.writeLock.lock(); 
        try { 
            if (LOG.isInfoEnabled()) 
                LOG.info("ForceWrite called -- current modifiedCounter: " 
                        + this.modifiedCounter + " - persisting changes"); 
            writePersistentIndex(true); 
            requestNewIndexModifier(); 
            this.modifiedCounter = 0; 
        } finally { 
            this.writeLock.unlock(); 
        } 
    } 
 
    private void requestNewIndexModifier() throws IOException, StorageException { 
        StorageCoreController controller = StorageCoreController 
                .getStorageCoreController(); 
        controller.registerNewStorageQuery(); 
        this.buffer = controller.releaseNewStorageBuffer(); 
        this.modifier = controller.createIndexModifier(); 
    } 
 
    private void writePersistentIndex(final boolean optimize) throws IOException { 
        try { 
            /* 
             * first delete all updated documents 
             */ 
            for(Term entryIdTerm : this.deletedForUpdateDocumentQueue) { 
                this.modifier.deleteDocuments(entryIdTerm);                
            } 
            /* 
             * add all documents 
             */ 
            Collection<Document> documents = this.documentMap.values(); 
            for (Document doc : documents) { 
                this.modifier.addDocument(doc); 
            } 
            /* 
             * delete all documents marked as deleted. As the DocumentIDs are 
             * unique the document marked as deleted must not persist after the 
             * index has been written. 
             * In the case of an update of a document and a previous delete the concurrency component will not allow an update. 
             * new inserted entries can not be deleted accidently- 
             */ 
            for (Term entryIdTerm : this.deletedDocumentQueue) { 
                this.modifier.deleteDocuments(entryIdTerm); 
            } 
            this.modifier.flush();  
            if(optimize){ 
                if(LOG.isInfoEnabled()) 
                    LOG.info("Optimizing index -- optimize interval "+this.optimizeInterval); 
                this.modifier.optimize(); 
            } 
 
        } finally { 
            if(optimize) 
                this.optimizeCounter = 0; 
            this.modifier.close(); 
            this.deletedForUpdateDocumentQueue.clear(); 
            this.deletedDocumentQueue.clear(); 
            this.documentMap.clear(); 
        } 
    } 
     
    protected void close()throws IOException{ 
        this.writeLock.lock(); 
        try { 
            if (LOG.isInfoEnabled()) 
                LOG.info("ForceWrite called -- current modifiedCounter: " 
                        + this.modifiedCounter + " - persisting changes"); 
 
            writePersistentIndex(true); 
            this.modifiedCounter = 0; 
        } finally { 
            this.writeLock.unlock(); 
        } 
    } 
     
    private void incrementCounter(){ 
        this.optimizeCounter++; 
        this.modifiedCounter++; 
    } 
 
} 
