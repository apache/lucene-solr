package org.apache.lucene.gdata.storage.lucenestorage; 
 
import java.io.File; 
import java.io.IOException; 
 
import org.apache.commons.logging.Log; 
import org.apache.commons.logging.LogFactory; 
import org.apache.lucene.analysis.standard.StandardAnalyzer; 
import org.apache.lucene.gdata.server.registry.GDataServerRegistry; 
import org.apache.lucene.gdata.storage.IDGenerator; 
import org.apache.lucene.gdata.storage.StorageController; 
import org.apache.lucene.gdata.storage.StorageException; 
import org.apache.lucene.gdata.storage.lucenestorage.configuration.StorageConfigurator; 
import org.apache.lucene.gdata.storage.lucenestorage.util.ReferenceCounter; 
import org.apache.lucene.index.IndexModifier; 
import org.apache.lucene.search.IndexSearcher; 
import org.apache.lucene.store.Directory; 
import org.apache.lucene.store.FSDirectory; 
 
/** 
 * TODO document this 
 * @author Simon Willnauer 
 * 
 */ 
public class StorageCoreController implements StorageController{ 
    protected static final Log LOG = LogFactory.getLog(StorageCoreController.class); 
    private IndexSearcher searcher; 
    private static StorageCoreController coreController; 
    private final Directory storageDir; 
    private final StorageModifier modifier; 
    private ReferenceCounter<StorageQuery> storageQuery; 
    private StorageBuffer currentBuffer; 
    private Object storageControllerLock = new Object(); 
    private static final int DEFAULT_STORAGE_BUFFER_SIZE = 10; 
    private static final int DEFAULT_STORAGE_PERSIST_FACTOR = 10; 
    private static final String STORAGELOG = ".lucenestorage"; 
    private int storageBufferSize; 
    private int storagePersistFactor; 
    private StorageConfigurator configurator; 
    private IDGenerator idGenerator; 
    private int indexOptimizeInterval; 
     
   private StorageCoreController()throws IOException, StorageException{ 
       this(null); 
   } 
    
    
    
    
    private StorageCoreController(final Directory dir) throws IOException, StorageException { 
        synchronized (StorageCoreController.class) { 
            try{ 
                this.idGenerator = new IDGenerator(10); 
            }catch (Exception e) { 
                throw new StorageException("Can't create ID Generator",e); 
            } 
             
            boolean createNewStorage = false; 
             
            if(dir == null){ 
            this.configurator = StorageConfigurator.getStorageConfigurator(); 
            String storageDirPath = this.configurator.getStorageDirectory(); 
            File storeDir = new File(storageDirPath); 
            File storageLog = new File(storeDir.getAbsolutePath()+System.getProperty("file.separator")+STORAGELOG); 
            try{ 
            if(storeDir.isDirectory() && !storageLog.exists()){ 
                 
                if(createLuceneStorageLog(storeDir)){ 
                    this.storageDir = FSDirectory.getDirectory(storeDir,true); 
                    createNewStorage = true; 
                } 
                else 
                    throw new StorageException("could not create storage log file in "+storageDirPath); 
                 
            }else 
                this.storageDir = FSDirectory.getDirectory(storeDir,false); 
            }catch (IOException e) { 
                storageLog.delete(); 
                throw e; 
            } 
            this.indexOptimizeInterval = this.configurator.getIndexOptimizeInterval(); 
            this.storageBufferSize = this.configurator.getStorageBufferSize() < DEFAULT_STORAGE_BUFFER_SIZE?DEFAULT_STORAGE_BUFFER_SIZE:this.configurator.getStorageBufferSize(); 
            this.storagePersistFactor = this.configurator.getStoragepersistFactor() < DEFAULT_STORAGE_PERSIST_FACTOR? DEFAULT_STORAGE_PERSIST_FACTOR:this.configurator.getStoragepersistFactor(); 
             
            } 
            else 
                this.storageDir = dir; 
             
            this.currentBuffer = new StorageBuffer(this.storageBufferSize); 
            this.modifier = createStorageModifier(createNewStorage); 
            this.searcher = new IndexSearcher(this.storageDir); 
             
 
            GDataServerRegistry.getRegistry().registerStorage(this);// TODO reverse dependency here 
            
                 
             
     } 
         
    } 
    private StorageModifier createStorageModifier(boolean create) throws IOException{ 
        IndexModifier indexModifier = new IndexModifier(this.storageDir,new StandardAnalyzer(),create); 
        return new StorageModifier(indexModifier,this.currentBuffer,this.storagePersistFactor,this.indexOptimizeInterval); 
    } 
    /**TODO document this 
     * @return 
     */ 
    public StorageModifier getStorageModifier(){ 
        return this.modifier; 
    } 
     
    /**TODO document this 
     * @return 
     * @throws IOException 
     * @throws StorageException 
     */ 
    public static StorageCoreController getStorageCoreController() throws IOException, StorageException{ 
        synchronized (StorageCoreController.class) { 
            if(coreController == null) 
            coreController = new StorageCoreController(); 
            return coreController; 
        } 
    } 
    /**TODO document this 
     * @param dir 
     * @return 
     * @throws IOException 
     * @throws StorageException 
     */ 
    protected static StorageCoreController getStorageCoreController(final Directory dir) throws IOException, StorageException{ 
        synchronized (StorageCoreController.class) { 
            if(coreController == null) 
            coreController = new StorageCoreController(dir); 
            return coreController; 
        } 
    } 
 
    /**TODO document this 
     * @return 
     * @throws IOException 
     */ 
    public  ReferenceCounter<StorageQuery> getStorageQuery() throws IOException { 
        synchronized (this.storageControllerLock) { 
 
        if(this.storageQuery == null){ 
            this.storageQuery = getNewStorageQueryHolder(new StorageQuery(this.currentBuffer,this.searcher)); 
            if(LOG.isInfoEnabled()) 
                LOG.info("Relese new StorageQuery"); 
        } 
        this.storageQuery.increamentReference(); 
        return this.storageQuery; 
        } 
    } 
     
    private ReferenceCounter<StorageQuery> getNewStorageQueryHolder(final StorageQuery query){ 
        ReferenceCounter<StorageQuery> holder = new ReferenceCounter<StorageQuery>(query){ 
            public void close(){ 
                try{ 
                    if(LOG.isInfoEnabled()) 
                        LOG.info("close StorageQuery -- zero references remaining"); 
                    this.resource.close(); 
                }catch (IOException e) { 
                    LOG.warn("Error during close call on StorageQuery"+e.getMessage(),e); 
                } 
            } 
        }; 
        holder.increamentReference(); 
        return holder; 
    } 
     
   
     
    protected void registerNewStorageQuery() throws IOException{ 
        if(LOG.isInfoEnabled()) 
            LOG.info("new StorageQuery requested -- create new storage buffer"); 
        synchronized (this.storageControllerLock) { 
            if(this.storageQuery != null) 
                this.storageQuery.decrementRef(); 
            this.searcher = new IndexSearcher(this.storageDir); 
            this.storageQuery = null; 
            this.currentBuffer = new StorageBuffer(this.storageBufferSize); 
             
        } 
         
    } 
 
     
    protected StorageBuffer releaseNewStorageBuffer() { 
        synchronized (this.storageControllerLock) { 
            return this.currentBuffer; 
        } 
    } 
 
    /**TODO document this 
     * @return 
     * @throws IOException 
     */ 
    public IndexModifier createIndexModifier() throws IOException { 
        if(LOG.isInfoEnabled()) 
            LOG.info("new IndexModifier created - release to StorageModifier"); 
        synchronized (this.storageControllerLock) { 
           return new IndexModifier(this.storageDir,new StandardAnalyzer(),false); 
        } 
    } 
     
    private void close() throws IOException{ 
        synchronized (this.storageControllerLock) { 
        if(LOG.isInfoEnabled()) 
            LOG.info("StorageController has been closed -- server is shutting down -- release all resources"); 
        if(this.storageQuery != null) 
            this.storageQuery.decrementRef(); 
        coreController = null; 
        this.modifier.close(); 
        //TODO make sure all resources will be released 
        } 
    } 
    /**TODO document this 
     * @return 
     */ 
    public int getStorageBufferSize() { 
        return this.storageBufferSize; 
    } 
    /** 
     * @param storageBufferSize 
     */ 
    public void setStorageBufferSize(int storageBufferSize) { 
        this.storageBufferSize = storageBufferSize; 
    } 
    /**TODO document this 
     * @return 
     */ 
    public int getStoragePersistFactor() { 
        return this.storagePersistFactor; 
    } 
    /** 
     * @param storagePersistFactor 
     */ 
    public void setStoragePersistFactor(int storagePersistFactor) { 
        this.storagePersistFactor = storagePersistFactor; 
    } 
    /** 
     * @throws IOException 
     * @throws StorageException 
     */ 
    public void forceWrite()throws IOException, StorageException{ 
        this.modifier.forceWrite(); 
    } 
     
     
    private boolean createLuceneStorageLog(File storageDirectory) throws IOException{ 
        if(storageDirectory.isDirectory() && !storageDirectory.exists()){ 
            storageDirectory.createNewFile(); 
        } 
        File file = new File(storageDirectory.getAbsolutePath()+System.getProperty("file.separator")+STORAGELOG); 
        return file.createNewFile(); 
         
         
    }    
     
    
    /**TODO document this 
     * @return 
     * @throws StorageException 
     */ 
    public synchronized String releaseID() throws StorageException{ 
        try{ 
        return this.idGenerator.getUID(); 
        }catch (InterruptedException e) { 
            throw new StorageException("Can't release new ID",e); 
        } 
         
    } 
 
 
 
    /** 
     * @see org.apache.lucene.gdata.storage.StorageController#destroy() 
     */ 
    public void destroy() { 
        try{ 
        close(); 
        }catch (Exception e) { 
            LOG.error("Closing StorageCoreController failed -- "+e.getMessage(),e); 
        } 
    } 
} 
