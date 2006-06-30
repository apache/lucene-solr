package org.apache.lucene.gdata.storage.lucenestorage;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.gdata.data.GDataAccount;
import org.apache.lucene.gdata.server.registry.Component;
import org.apache.lucene.gdata.server.registry.ComponentType;
import org.apache.lucene.gdata.storage.IDGenerator;
import org.apache.lucene.gdata.storage.Storage;
import org.apache.lucene.gdata.storage.StorageController;
import org.apache.lucene.gdata.storage.StorageException;
import org.apache.lucene.gdata.storage.lucenestorage.configuration.StorageConfigurator;
import org.apache.lucene.gdata.storage.lucenestorage.util.ReferenceCounter;
import org.apache.lucene.index.IndexModifier;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;

/**
 *  
 * 
 * @author Simon Willnauer
 * 
 */
@Component(componentType = ComponentType.STORAGECONTROLLER)
public class StorageCoreController implements StorageController {
    protected static final Log LOG = LogFactory
            .getLog(StorageCoreController.class);

    private IndexSearcher searcher;

    private final Directory storageDir;

    private final StorageModifier modifier;

    private ReferenceCounter<StorageQuery> storageQuery;

    private StorageBuffer currentBuffer;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final ReentrantLock storageControllerLock = new ReentrantLock();
    private final Condition closeCondition;

    private static final int DEFAULT_STORAGE_BUFFER_SIZE = 10;

    private static final int DEFAULT_STORAGE_PERSIST_FACTOR = 10;

    private static final String STORAGELOG = ".lucenestorage";

    private int storageBufferSize;

    private int storagePersistFactor;

    private StorageConfigurator configurator;

    private IDGenerator idGenerator;

    private int indexOptimizeInterval;
    
//    private RecoverController recoverController;

    /**
     * Creates a new <tt>StoragCoreController</tt> and sets up the storage
     * environment reading the configuration file.
     * 
     * 
     * 
     * @throws IOException -
     *             if an IOException occures
     * @throws StorageException -
     *             if the storage lock can not be created or the
     *             {@link IDGenerator} can not be loaded
     */
    public StorageCoreController() throws IOException, StorageException {
        synchronized (StorageCoreController.class) {
        	this.closeCondition = this.storageControllerLock.newCondition();
            try {
                this.idGenerator = new IDGenerator(10);
            } catch (Exception e) {
                throw new StorageException("Can't create ID Generator", e);
            }

            boolean createNewStorage = false;
            this.configurator = StorageConfigurator.getStorageConfigurator();
            if (!this.configurator.isRamDirectory()) {

                String storageDirPath = this.configurator.getStorageDirectory();
                File storeDir = new File(storageDirPath);
                File storageLog = new File(storeDir.getAbsolutePath()
                        + System.getProperty("file.separator") + STORAGELOG);
                try {
                    if (storeDir.isDirectory() && !storageLog.exists()) {

                        if (createLuceneStorageLog(storeDir)) {
                            this.storageDir = FSDirectory.getDirectory(
                                    storeDir, true);
                            createNewStorage = true;
                        } else
                            throw new StorageException(
                                    "could not create storage lock file in "
                                            + storageDirPath);

                    } else
                        this.storageDir = FSDirectory.getDirectory(storeDir,
                                false);
                } catch (IOException e) {
                    storageLog.delete();
                    throw e;
                }
                this.indexOptimizeInterval = this.configurator
                        .getIndexOptimizeInterval();
                this.storageBufferSize = this.configurator
                        .getStorageBufferSize() < DEFAULT_STORAGE_BUFFER_SIZE ? DEFAULT_STORAGE_BUFFER_SIZE
                        : this.configurator.getStorageBufferSize();
                this.storagePersistFactor = this.configurator
                        .getStoragepersistFactor() < DEFAULT_STORAGE_PERSIST_FACTOR ? DEFAULT_STORAGE_PERSIST_FACTOR
                        : this.configurator.getStoragepersistFactor();

            } else
                this.storageDir = getRamDirectory();

            this.currentBuffer = new StorageBuffer(this.storageBufferSize);
            this.modifier = createStorageModifier(createNewStorage);
            this.searcher = new IndexSearcher(this.storageDir);
//            this.recoverController = new RecoverController(null,this.configurator.isRecover(),this.configurator.isKeepRecoveredFiles());
            if(createNewStorage)
                createAdminAccount();

        }

    }

    private StorageModifier createStorageModifier(boolean create)
            throws IOException {
        IndexModifier indexModifier = new IndexModifier(this.storageDir,
                new StandardAnalyzer(), create);
        return new StorageModifier(this, indexModifier, this.currentBuffer,
                this.storagePersistFactor, this.indexOptimizeInterval);
    }

    /**
     * returns the current storage modifier
     * 
     * @return - the current storage modifier
     */
    protected StorageModifier getStorageModifier() {
        return this.modifier;
    }

    /**
     * returns a <tt>StorageQuery</tt> to query the storage index. The
     * returned object is a reference counter to keep track of the references to
     * the <tt>StorageQuery</tt>. The reference is already incremented before
     * returned from this method.
     * <p>
     * if the reference counter has no remaining references the resource e.g.
     * the <tt>StorageQuery</tt> will be closed. This ensures that a
     * <tt>StorageQuery</tt> instance will be arround as long as needed and
     * the resources will be released. The reference counter should be
     * decremented by clients after finished using the query instance.
     * </p>
     * 
     * @return a {@link ReferenceCounter} instance holding the StorageQuery as a
     *         resource.
     * 
     */
    protected ReferenceCounter<StorageQuery> getStorageQuery() {
    	if(this.isClosed.get())
    		throw new IllegalStateException("StorageController is already closed -- server is shutting down");
        this.storageControllerLock.lock();
        try{
        	if(this.isClosed.get())
        		throw new IllegalStateException("StorageController is already closed -- server is shutting down");
            if (this.storageQuery == null) {
                this.storageQuery = getNewStorageQueryHolder(new StorageQuery(
                        this.currentBuffer, this.searcher));
                if (LOG.isInfoEnabled())
                    LOG.info("Relese new StorageQuery");
            }
            this.storageQuery.increamentReference();
            return this.storageQuery;
        }finally{
        	this.closeCondition.signalAll();
        	this.storageControllerLock.unlock();
        }
    }

    private ReferenceCounter<StorageQuery> getNewStorageQueryHolder(
            final StorageQuery query) {
        ReferenceCounter<StorageQuery> holder = new ReferenceCounter<StorageQuery>(
                query) {
            @Override
			public void close() {
                try {
                    if (LOG.isInfoEnabled())
                        LOG
                                .info("close StorageQuery -- zero references remaining");
                    this.resource.close();
                } catch (IOException e) {
                    LOG.warn("Error during close call on StorageQuery"
                            + e.getMessage(), e);
                }
            }
        };
        holder.increamentReference();
        return holder;
    }

    /**
     * Forces the controller to register a new <tt>StorageQuery</tt> instance.
     * This method will be called after an index has been modified to make the
     * changes available for searching.
     * 
     * @throws IOException -
     *             if an IO exception occures
     */
    protected void registerNewStorageQuery() throws IOException {
    	if(this.isClosed.get())
    		throw new IllegalStateException("StorageController is already closed -- server is shutting down");
        this.storageControllerLock.lock();
        try{
        	if(this.isClosed.get())
        		throw new IllegalStateException("StorageController is already closed -- server is shutting down");
	        if (LOG.isInfoEnabled())
	            LOG.info("new StorageQuery requested -- create new storage buffer");
        
            if (this.storageQuery != null)
                this.storageQuery.decrementRef();
            this.searcher = new IndexSearcher(this.storageDir);
            this.storageQuery = null;
            this.currentBuffer = new StorageBuffer(this.storageBufferSize);

        }finally{
        	this.closeCondition.signalAll();
        	this.storageControllerLock.unlock();
        }

    }

    /**
     * Creates a new StorageBuffer
     * 
     * @return the new StorageBuffer
     */
    protected StorageBuffer releaseNewStorageBuffer() {
    	if(this.isClosed.get())
    		throw new IllegalStateException("StorageController is already closed -- server is shutting down");
        this.storageControllerLock.lock();
        try{
        	if(this.isClosed.get())
        		throw new IllegalStateException("StorageController is already closed -- server is shutting down");
            return this.currentBuffer;
        }finally{
        	this.closeCondition.signalAll();
        	this.storageControllerLock.unlock();
        }
    }

    /**
     * Creates a new IndexModifier on the storage index
     * 
     * @return - a new modifier
     * @throws IOException -
     *             if an IO exception occures
     */
    protected IndexModifier createIndexModifier() throws IOException {
    	if(this.isClosed.get())
    		throw new IllegalStateException("StorageController is already closed -- server is shutting down");
        this.storageControllerLock.lock();
        try{
        	if(this.isClosed.get())
        		throw new IllegalStateException("StorageController is already closed -- server is shutting down");
	        if (LOG.isInfoEnabled())
	            LOG.info("new IndexModifier created - release to StorageModifier");
        
            return new IndexModifier(this.storageDir, new StandardAnalyzer(),
                    false);
        }finally{
        	this.closeCondition.signalAll();
        	this.storageControllerLock.unlock();
        }
    }

    private void close() throws IOException {
    	if(this.isClosed.get())
    		throw new IllegalStateException("StorageController is already closed -- server is shutting down");
    	
        this.storageControllerLock.lock();
        try{
        	if(this.isClosed.get())
        		throw new IllegalStateException("StorageController is already closed -- server is shutting down");
        	this.isClosed.set(true);
        	while(this.storageControllerLock.getQueueLength()>0)
        		try{
        		this.closeCondition.await();
        		}catch (Exception e) {
					//
				}
            if (LOG.isInfoEnabled())
                LOG.info("StorageController has been closed -- server is shutting down -- release all resources");
            if (this.storageQuery != null)
                this.storageQuery.decrementRef();
            this.modifier.close();
		}finally{
        	this.storageControllerLock.unlock();
        }
    }

    /**
     * The size of the <tt>StorageBuffer</tt>.
     * 
     * @return - storage buffer size
     */
    public int getStorageBufferSize() {
        return this.storageBufferSize;
    }

    /**
     * The size of the <tt>StorageBuffer</tt>. This size should be at least
     * as big as the persist factor to prevent the <tt>StorageBuffer</tt> from
     * resizing
     * 
     * @param storageBufferSize
     */
    public void setStorageBufferSize(int storageBufferSize) {
        this.storageBufferSize = storageBufferSize;
    }

    /**
     * An integer value after how many changes to the StorageModifier the
     * buffered changes will be persisted / wirtten to the index
     * 
     * @return - the persist factor
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
     * Forces the StorageModifier to write all buffered changes.
     * 
     * @throws IOException -
     *             if an IO exception occures
     * 
     */
    public void forceWrite() throws IOException {
    	if(this.isClosed.get())
    		throw new IllegalStateException("StorageController is already closed -- server is shutting down");
        this.modifier.forceWrite();
    }

    private boolean createLuceneStorageLog(File storageDirectory)
            throws IOException {
        if (storageDirectory.isDirectory() && !storageDirectory.exists()) {
            storageDirectory.createNewFile();
        }
        File file = new File(storageDirectory.getAbsolutePath()
                + System.getProperty("file.separator") + STORAGELOG);
        return file.createNewFile();

    }

    /**
     * Creates a unique ID to store as an id for
     * {@link org.apache.lucene.gdata.data.ServerBaseEntry} instances
     * 
     * @return - a unique id
     * @throws StorageException -
     *             if no id can be released
     */
    public synchronized String releaseID() throws StorageException {
        try {
            return this.idGenerator.getUID();
        } catch (InterruptedException e) {
            throw new StorageException("Can't release new ID", e);
        }

    }

    /**
     * @see org.apache.lucene.gdata.storage.StorageController#destroy()
     */
    public void destroy() {
        try {
            close();
        } catch (Exception e) {
            LOG.error("Closing StorageCoreController failed -- "
                    + e.getMessage(), e);
        }
    }

    /**
     * 
     * @return - the lucene directory used as a storage
     */
    protected Directory getDirectory() {
        return this.storageDir;
    }

    /**
     * @see org.apache.lucene.gdata.storage.StorageController#getStorage()
     */
    public Storage getStorage() throws StorageException {
        try {
            return new StorageImplementation();
        } catch (StorageException e) {
            StorageException ex = new StorageException(
                    "Can't create Storage instance -- " + e.getMessage(), e);
            ex.setStackTrace(e.getStackTrace());
            throw ex;

        }
    }

    // TODO Try to remove this --> testcases
    private RAMDirectory getRamDirectory() throws IOException {
        IndexWriter writer;
        RAMDirectory retVal = new RAMDirectory();
        writer = new IndexWriter(retVal, new StandardAnalyzer(), true);
        writer.close();
        return retVal;
    }

    /**
     * @see org.apache.lucene.gdata.server.registry.ServerComponent#initialize()
     */
    public void initialize() {
        //
    }
    
    private void createAdminAccount() throws StorageException{
        GDataAccount adminAccount = GDataAccount.createAdminAccount();
        StorageAccountWrapper wrapper = new StorageAccountWrapper(adminAccount);
        this.getStorageModifier().createAccount(wrapper);
    }

}
