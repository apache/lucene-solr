/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.lucene.gdata.server.registry.configuration.Requiered;
import org.apache.lucene.gdata.storage.IDGenerator;
import org.apache.lucene.gdata.storage.Storage;
import org.apache.lucene.gdata.storage.StorageController;
import org.apache.lucene.gdata.storage.StorageException;
import org.apache.lucene.gdata.storage.lucenestorage.recover.RecoverController;
import org.apache.lucene.gdata.storage.lucenestorage.recover.RecoverException;
import org.apache.lucene.gdata.utils.ReferenceCounter;
import org.apache.lucene.index.IndexModifier;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

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

    private Directory storageDir;

    private StorageModifier modifier;

    private ReferenceCounter<StorageQuery> storageQuery;

    private StorageBuffer currentBuffer;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final ReentrantLock storageControllerLock = new ReentrantLock();
    private final Condition closeCondition;

    private static final int DEFAULT_STORAGE_BUFFER_SIZE = 3;

    private static final int DEFAULT_STORAGE_PERSIST_FACTOR = 3;
    
    private static final String RECOVERDIRECTORY = "recover";

    private static final String STORAGELOG = ".lucenestorage";

    private IDGenerator idGenerator;

    private final ConcurrentStorageLock storageLock;
    /*
     *properties set by configuration file e.g. Registry
     */
    private int indexOptimizeInterval;
    
    private String storageDirectory; 
 
    private boolean keepRecoveredFiles; 
 
    private boolean recover; 
    
    private int storageBufferSize;

    private int storagePersistFactor;
    
    private RecoverController recoverController;
    /**
     * @see org.apache.lucene.gdata.server.registry.ServerComponent#initialize()
     */
    public void initialize() {
        synchronized (StorageCoreController.class) {
         
            try {
                this.idGenerator = new IDGenerator(10);
            } catch (Exception e) {
                throw new StorageException("Can't create ID Generator", e);
            }

            boolean createNewStorage = false;
          
            if (this.storageDir == null) {

                
                File storeDir = new File(this.storageDirectory);
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
                                            + this.storageDirectory);

                    } else
                        this.storageDir = FSDirectory.getDirectory(storeDir,
                                false);
                } catch (IOException e) {
                    storageLog.delete();
                    throw new StorageException(e);
                }
                
                this.storageBufferSize = this.storageBufferSize < DEFAULT_STORAGE_BUFFER_SIZE ? DEFAULT_STORAGE_BUFFER_SIZE
                        : this.storageBufferSize;
                this.storagePersistFactor = this.storagePersistFactor < DEFAULT_STORAGE_PERSIST_FACTOR ? DEFAULT_STORAGE_PERSIST_FACTOR
                        : this.storagePersistFactor;

            }else
                createNewStorage = true;
               

            this.currentBuffer = new StorageBuffer(this.storageBufferSize);
            try{
            this.modifier = createStorageModifier(createNewStorage);
            this.searcher = new IndexSearcher(this.storageDir);
            }catch (Exception e) {
               throw new StorageException("Can not create Searcher/Modifier -- "+e.getMessage(),e);
            }
           
            
            if(createNewStorage)
                createAdminAccount();
            if(!this.recover)
                return;
            try{
            tryRecover();
            }catch (Exception e) {
                LOG.fatal("Recovering failed",e);
                throw new StorageException("Recovering failed -- "+e.getMessage(),e); 
            }
            
            this.recoverController = createRecoverController(false,false);
            try{
            this.recoverController.initialize();
            }catch (Exception e) {
                LOG.fatal("Can not initialize recover controller",e);
                throw new StorageException("Can not initialize recover controller -- "+e.getMessage(),e);
            }

        }
    }
    /*
     * reads the remaining recover files to store the failed entries
     */
    private void tryRecover() throws IOException, RecoverException{
        if(!this.recover)
            return;
        LOG.info("try to recover files if there are any");
        this.recoverController = createRecoverController(true,false);
        this.recoverController.initialize();
        this.recoverController.recoverEntries(this.modifier);
        this.recoverController.destroy();
    }
    
    private RecoverController createRecoverController(boolean doRecover, boolean keepfiles){
        String recoverDirectory = null;
        if(this.storageDirectory.endsWith("/") || this.storageDirectory.endsWith("\\"))
            recoverDirectory = this.storageDirectory.substring(0,this.storageDirectory.length()-1)+System.getProperty("file.separator")+RECOVERDIRECTORY;
        else
            recoverDirectory = this.storageDirectory+System.getProperty("file.separator")+RECOVERDIRECTORY;
        File recoverDirectoryFile = new File(recoverDirectory);
       return new RecoverController(recoverDirectoryFile,doRecover,keepfiles);
    }
    /**
     * Creates a new <tt>StoragCoreController</tt>
     */
    public StorageCoreController() {
        this.closeCondition = this.storageControllerLock.newCondition();
        this.storageLock = SingleHostConcurrentStorageLock.getConcurrentStorageLock();

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
     * <tt>StorageQuery</tt> instance will be around as long as needed and
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
                    LOG.info("Release new StorageQuery");
            }
            this.storageQuery.increamentReference();
            return this.storageQuery;
        }finally{
            try{
        	this.closeCondition.signalAll();
            }catch (Throwable e) {/**/}
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
     *             if an IO exception occurs
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
            try{
                this.closeCondition.signalAll();
                }catch (Throwable e) {/**/}
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
            try{
        	this.closeCondition.signalAll();
            }catch (Throwable e) {/**/}
        	this.storageControllerLock.unlock();
        }
    }

    /**
     * Creates a new IndexModifier on the storage index
     * 
     * @return - a new modifier
     * @throws IOException -
     *             if an IO exception occurs
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
            try{
                this.closeCondition.signalAll();
                }catch (Throwable e) {/**/}
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
            if(this.recoverController != null)
                this.recoverController.destroy();
            this.storageLock.close();
            this.modifier.close();
            this.idGenerator.stopIDGenerator();
		}finally{
        	this.storageControllerLock.unlock();
        }
    }


    /**
     * Forces the StorageModifier to write all buffered changes.
     * 
     * @throws IOException -
     *             if an IO exception occurs
     * 
     */
    public void forceWrite() throws IOException {
    	if(this.isClosed.get())
    		throw new IllegalStateException("StorageController is already closed -- server is shutting down");
        this.modifier.forceWrite();
    }

    private boolean createLuceneStorageLog(File directory)
            throws IOException {
        if (directory.isDirectory() && !directory.exists()) {
            if(!directory.createNewFile())
                throw new StorageException("Can not create directory -- "+directory);
        }
        File file = new File(directory.getAbsolutePath()
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
    public synchronized String releaseId() {
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




    
    
    private void createAdminAccount() throws StorageException{
        GDataAccount adminAccount = GDataAccount.createAdminAccount();
        StorageAccountWrapper wrapper = new StorageAccountWrapper(adminAccount);
        this.getStorageModifier().createAccount(wrapper);
    }
    
    protected ConcurrentStorageLock getLock(){
        return this.storageLock;
    }

    
    /**
     * The size of the <tt>StorageBuffer</tt>.
     * 
     * @return - storage buffer size
     */
    public int getBufferSize() {
        return this.storageBufferSize;
    }

    /**
     * The size of the <tt>StorageBuffer</tt>. This size should be at least
     * as big as the persist factor to prevent the <tt>StorageBuffer</tt> from
     * resizing
     * 
     * @param storageBufferSize
     */
    @Requiered
    public void setBufferSize(int storageBufferSize) {
        this.storageBufferSize = storageBufferSize;
    }

    /**
     * An integer value after how many changes to the StorageModifier the
     * buffered changes will be persisted / written to the index
     * 
     * @return - the persist factor
     */
    public int getPersistFactor() {
        return this.storagePersistFactor;
    }

    /**
     * @param storagePersistFactor
     */
    @Requiered
    public void setPersistFactor(int storagePersistFactor) {
        this.storagePersistFactor = storagePersistFactor;
    }


    /**
     * @return Returns the indexOptimizeInterval.
     */
    public int getIndexOptimizeInterval() {
        return this.indexOptimizeInterval;
    }


    /**
     * @param indexOptimizeInterval The indexOptimizeInterval to set.
     */
    @Requiered
    public void setOptimizeInterval(int indexOptimizeInterval) {
        this.indexOptimizeInterval = indexOptimizeInterval;
    }


    /**
     * @return Returns the keepRecoveredFiles.
     */
    public boolean isKeepRecoveredFiles() {
        return this.keepRecoveredFiles;
    }


    /**
     * @param keepRecoveredFiles The keepRecoveredFiles to set.
     */
    @Requiered
    public void setKeepRecoveredFiles(boolean keepRecoveredFiles) {
        this.keepRecoveredFiles = keepRecoveredFiles;
    }



    /**
     * @return Returns the recover.
     */
    public boolean isRecover() {
        return this.recover;
    }


    /**
     * @param recover The recover to set.
     */
    @Requiered
    public void setRecover(boolean recover) {
        this.recover = recover;
    }


    /**
     * @param storageDir The storageDir to set.
     */
   
    public void setStorageDir(Directory storageDir) {
        this.storageDir = storageDir;
    }


    /**
     * @param storageDirectory The storageDirectory to set.
     */
    @Requiered
    public void setDirectory(String storageDirectory) {
        this.storageDirectory = storageDirectory;
    }
    
    protected void writeRecoverEntry(StorageEntryWrapper wrapper) throws RecoverException{
        if(this.recoverController!= null &&!this.recoverController.isRecovering() )
            this.recoverController.storageModified(wrapper);
    }
    protected void registerNewRecoverWriter() throws IOException {
        if(this.recoverController == null || this.recoverController.isRecovering())
            return;
        this.recoverController.destroy();
        this.recoverController = createRecoverController(false,false);
        this.recoverController.initialize();
            
    }

}
