package org.apache.lucene.gdata.storage.lucenestorage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.document.Document;
import org.apache.lucene.gdata.storage.StorageException;
import org.apache.lucene.gdata.storage.lucenestorage.StorageEntryWrapper.StorageOperation;
import org.apache.lucene.index.IndexModifier;
import org.apache.lucene.index.Term;

/**
 * The StorageModifier is the a Singleton component of the LuceneStorage. There
 * is one single instance of this class modifying the index used to store all
 * the gdata Entities as Entries, Feeds and Users. This class contains an
 * instance of {@link org.apache.lucene.index.IndexModifier} used to manage all
 * delete and add actions to the storage.
 * <p>
 * To prevent the storage component from opening and closing the
 * {@link org.apache.lucene.index.IndexModifier} for every modifying operation
 * the incoming entry actions (DELETE, UPDATE, INSERT) will be buffered in a
 * registered instance of
 * {@link org.apache.lucene.gdata.storage.lucenestorage.StorageBuffer}. When a
 * certain amout (specified as the persistfactor in the configuration file) of
 * modifications have been executed the StorageModifier will persist the
 * buffered entries.
 * </p>
 * <p>
 * Feed and User operations won't be buffered. These actions occure not very
 * often compared to entry actions. Every call of an user / feed modifying
 * operation forces all changes to be written to the storage index.
 * </p>
 * 
 * @author Simon Willnauer
 * 
 */
public class StorageModifier {
    protected static final Log LOG = LogFactory.getLog(StorageModifier.class);

    private final List<Term> deletedDocumentQueue;

    private final List<Term> deletedForUpdateDocumentQueue;

    private final Map<String, Document> documentMap;

    private final List<Document> forceWriteDocuments;

    private final List<Term> forceWriteTerms;

    private volatile int persistFactor;

    private volatile int modifiedCounter = 0;

    private static int DEFAULT_PERSIST_FACTOR = 10;

    private StorageBuffer buffer;

    private IndexModifier modifier;

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock(false);
    
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    
    private final Lock readLock = this.lock.readLock();

    private final Lock writeLock = this.lock.writeLock();

    private final static int DEFAULT_OPTIMIZE_INTERVAL = 10;

    private final int optimizeInterval;

    private volatile int optimizeCounter = 0;

    private final StorageCoreController controller;

    /**
     * Creates a new StorageModifier
     * 
     * @param controller -
     *            the registered StorageController
     * @param modifier -
     *            the IndexModifier
     * @param buffer -
     *            the StorageBuffer
     * @param persitsFactor -
     *            the factor when the changes will be persisted to the storage
     *            index
     * @param optimizeInterval -
     *            after how many storage operations the index will be optimized
     */
    protected StorageModifier(final StorageCoreController controller,
            final IndexModifier modifier, final StorageBuffer buffer,
            int persitsFactor, int optimizeInterval) {
        this.deletedDocumentQueue = new LinkedList<Term>();
        this.deletedForUpdateDocumentQueue = new LinkedList<Term>();
        this.documentMap = new HashMap<String, Document>(persitsFactor);
        this.forceWriteDocuments = new ArrayList<Document>(5);
        this.forceWriteTerms = new ArrayList<Term>(5);
        this.buffer = buffer;
        this.controller = controller;

        this.persistFactor = persitsFactor > 0 ? persitsFactor
                : DEFAULT_PERSIST_FACTOR;
        this.modifier = modifier;
        this.optimizeInterval = optimizeInterval < DEFAULT_OPTIMIZE_INTERVAL ? DEFAULT_OPTIMIZE_INTERVAL
                : optimizeInterval;

    }

    /**
     * Updates the given entry. First the alredy persisted entry will be
     * removed, after marking as deleted the new Entry will be written.
     * 
     * @param wrapper -
     *            the wrapper containing the entry
     * @throws StorageException -
     *             if the entry can not be stored
     */
    public void updateEntry(StorageEntryWrapper wrapper)
            throws StorageException {
        if(wrapper.getOperation() != StorageOperation.UPDATE)
            throw new StorageException("Illegal method call -- updateEntry does not accept other storageOperations than update");
        this.readLock.lock();
        try {
            Term tempTerm = new Term(StorageEntryWrapper.FIELD_ENTRY_ID,
                    wrapper.getEntryId());
            this.documentMap.put(wrapper.getEntryId(), wrapper
                    .getLuceneDocument());
            this.deletedForUpdateDocumentQueue.add(tempTerm);
            this.buffer.addEntry(wrapper);
            storageModified();
        } finally {
            this.readLock.unlock();
        }
    }

    /**
     * Inserts a new Entry to the Lucene index storage
     * 
     * @param wrapper -
     *            the wrapper containing the entry
     * @throws StorageException -
     *             if the entry can not be stored
     */
    public void insertEntry(StorageEntryWrapper wrapper)
            throws StorageException {
        if(wrapper.getOperation() != StorageOperation.INSERT)
            throw new StorageException("Illegal method call -- insertEntry does not accept other storage operations than insert");
        this.readLock.lock();
        try {
            this.documentMap.put(wrapper.getEntryId(), wrapper
                    .getLuceneDocument());
            this.buffer.addEntry(wrapper);
            storageModified();
        } finally {
            this.readLock.unlock();
        }
    }

    /**
     * Deletes the entry for the given entry id.
     * @param wrapper - the wrapper containing the information to delete 
     * 
     * @throws StorageException -
     *             if the entry can not be deleted
     * 
     */
    public void deleteEntry(final StorageEntryWrapper wrapper)
            throws StorageException {
        if(wrapper.getOperation() != StorageOperation.DELETE)
            throw new StorageException("Illegal method call -- insertEntry does not accept other storage operations than delete");
        this.readLock.lock();
        try {
            Term tempTerm = new Term(StorageEntryWrapper.FIELD_ENTRY_ID,
                    wrapper.getEntryId());
            this.deletedDocumentQueue.add(tempTerm);
            this.buffer.addDeleted(wrapper.getEntryId(), wrapper.getFeedId());
            storageModified();
        } finally {
            this.readLock.unlock();
        }
    }

    /**
     * Adds a new Feed to the storage. Feed action will be not buffered. Call to
     * this method forces the index to be written.
     * 
     * @param wrapper -
     *            the wrapper containing the feed;
     * @throws StorageException -
     *             if the feed can not be written
     */
    public void createFeed(StorageFeedWrapper wrapper) throws StorageException {
        this.readLock.lock();
        try {
            this.forceWriteDocuments.add(wrapper.getLuceneDocument());
            storageModified();
        } finally {
            this.readLock.unlock();
        }
    }

    /**
     * Adds a new accountr to the storage. User action will be not buffered. Call to
     * this method forces the index to be written.
     * 
     * @param account
     *            -the wrapper containig the user to be persisted
     * @throws StorageException -
     *             if the user can not be persisted.
     */
    public void createAccount(StorageAccountWrapper account) throws StorageException {
        this.readLock.lock();
        try {
            this.forceWriteDocuments.add(account.getLuceneDocument());
            storageModified();
        } finally {
            this.readLock.unlock();
        }
    }

    /**
     * Deletes the user with the given username. User action will be not
     * buffered. Call to this method forces the index to be written.
     * 
     * @param accountName -
     *            the user to be deleted
     * @throws StorageException -
     *             If the user could not be deleted
     */
    public void deleteAccount(String accountName) throws StorageException {
        this.readLock.lock();
        try {
            //TODO delete all feeds and entries of this account
            this.forceWriteTerms.add(new Term(
                    StorageAccountWrapper.FIELD_ACCOUNTNAME, accountName));
            storageModified();
        } finally {
            this.readLock.unlock();
        }
    }

    /**
     * User action will be not buffered. Call to this method forces the index to
     * be written.
     * 
     * @param user
     *            -the wrapper containig the user to be persisted
     * @throws StorageException -
     *             if the user can not be persisted.
     */
    public void updateAccount(final StorageAccountWrapper user)
            throws StorageException {
        this.readLock.lock();
        try {
            this.forceWriteTerms.add(new Term(
                    StorageAccountWrapper.FIELD_ACCOUNTNAME, user.getUser()
                            .getName()));
            this.forceWriteDocuments.add(user.getLuceneDocument());
            storageModified();
        } finally {
            this.readLock.unlock();
        }
    }

    /**
     * Feed action will be not buffered. Call to this method forces the index to
     * be written.
     * 
     * @param wrapper -
     *            the wrapper containig the feed
     * @throws StorageException -
     *             if the feed can not be persisted
     */
    public void updateFeed(final StorageFeedWrapper wrapper)
            throws StorageException {
        this.readLock.lock();
        try {
            this.forceWriteTerms.add(new Term(StorageFeedWrapper.FIELD_FEED_ID,
                    wrapper.getFeed().getId()));
            this.forceWriteDocuments.add(wrapper.getLuceneDocument());
            storageModified();
        } finally {
            this.readLock.unlock();
        }
    }

    /**
     * Deletes the feed with the given feed id Feed action will be not buffered.
     * Call to this method forces the index to be written.
     * All entries referencing the given feed id will be deleted as well!
     * @param feedId -
     *            the id of the feed to delete
     * @throws StorageException -
     *             if the feed can not be deleted
     */
    public void deleteFeed(final String feedId) throws StorageException {
        this.readLock.lock();
        try {
            this.deletedDocumentQueue.add(new Term(StorageEntryWrapper.FIELD_FEED_REFERENCE,feedId));
            this.forceWriteTerms.add(new Term(StorageFeedWrapper.FIELD_FEED_ID,
                    feedId));

            storageModified();
        } finally {
            this.readLock.unlock();
        }
    }

    private void storageModified() throws StorageException {
    	if(this.isClosed.get())
    		throw new IllegalStateException("StorageModifier is already closed");
        this.readLock.unlock();
        this.writeLock.lock();

        try {
        	if(this.isClosed.get())
        		throw new IllegalStateException("StorageModifier is already closed");
            incrementCounter();
            if (this.persistFactor > this.modifiedCounter
                    && this.forceWriteDocuments.size() <= 0
                    && this.forceWriteTerms.size() <= 0)
                return;

            if (LOG.isInfoEnabled())
                LOG.info("Storage modified for " + this.modifiedCounter
                        + " times. Write Persistent index");
            writePersistentIndex((this.optimizeCounter >= this.optimizeInterval));
            requestNewIndexModifier();

            this.modifiedCounter = 0;
        } catch (IOException e) {

            LOG.error("Writing persistent index failed - Recovering", e);
            throw new StorageException("could not write to storage index -- "+e.getMessage(),e);

        } finally {
            this.readLock.lock();
            this.writeLock.unlock();
        }

    }

    protected void forceWrite() throws IOException {
    	if(this.isClosed.get())
    		throw new IllegalStateException("StorageModifier is already closed");
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

    private void requestNewIndexModifier() throws IOException {

        this.controller.registerNewStorageQuery();
        this.buffer = this.controller.releaseNewStorageBuffer();
        this.modifier = this.controller.createIndexModifier();
    }

    private void writePersistentIndex(final boolean optimize)
            throws IOException {
        try {

            /*
             * first delete all updated documents
             */
            for (Term entryIdTerm : this.deletedForUpdateDocumentQueue) {
                this.modifier.deleteDocuments(entryIdTerm);
            }

            for (Term term : this.forceWriteTerms) {
                this.modifier.deleteDocuments(term);
            }
            /*
             * add all documents
             */
            Collection<Document> documents = this.documentMap.values();
            for (Document doc : documents) {
                this.modifier.addDocument(doc);
            }
            /*
             * write all users or feeds
             */
            for (Document docs : this.forceWriteDocuments) {
                this.modifier.addDocument(docs);
            }

            /*
             * delete all documents marked as deleted. As the DocumentIDs are
             * unique the document marked as deleted must not persist after the
             * index has been written. In the case of an update of a document
             * and a previous delete the concurrency component will not allow an
             * update. new inserted entries can not be deleted accidently-
             */
            for (Term entryIdTerm : this.deletedDocumentQueue) {
                this.modifier.deleteDocuments(entryIdTerm);
            }
            this.modifier.flush();
            if (optimize) {
                if (LOG.isInfoEnabled())
                    LOG.info("Optimizing index -- optimize interval "
                            + this.optimizeInterval);
                this.modifier.optimize();
            }

        } finally {
            if (optimize)
                this.optimizeCounter = 0;
            this.modifier.close();
            this.deletedForUpdateDocumentQueue.clear();
            this.deletedDocumentQueue.clear();
            this.documentMap.clear();
            this.forceWriteDocuments.clear();
            this.forceWriteTerms.clear();
        }
    }

    protected void close() throws IOException {
    	if(this.isClosed.get())
    		throw new IllegalStateException("StorageModifier is already closed");
        this.writeLock.lock();
        try {
        	if(this.isClosed.get())
        		throw new IllegalStateException("StorageModifier is already closed");
        	this.isClosed.set(true);
            if (LOG.isInfoEnabled())
                LOG.info("ForceWrite called -- current modifiedCounter: "
                        + this.modifiedCounter + " - persisting changes");

            writePersistentIndex(true);
            this.modifiedCounter = 0;
        } finally {
            this.writeLock.unlock();
        }
    }

    private void incrementCounter() {
        this.optimizeCounter++;
        this.modifiedCounter++;
    }

}
