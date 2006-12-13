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
package org.apache.lucene.gdata.search.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.gdata.search.config.IndexSchema;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;

/**
 * A GDataIndexer encapsulates every writing access to the search index.
 * <p>
 * Insert, updates and deletes to the index happens inside this class. All
 * modification will be base on an instance of
 * {@link org.apache.lucene.gdata.search.index.IndexDocument} which contains all
 * informations and command for the indexer.<br>
 * Although this class provides methods to add, remove and update document in
 * the index all <tt>IndexDocument</tt> instances should be added to the task
 * queue via the {@link GDataIndexer#addIndexableDocumentTask(Future)} method.
 * Inside this class runs an instance of
 * {@link org.apache.lucene.gdata.search.index.IndexTask} listening on this
 * queue. The analysis of the actual documents happens inside the
 * {@link com.sun.corba.se.impl.orbutil.closure.Future} object added to the
 * queue. This enables the indexer to do his actual work. Documents will be
 * build / analyzed concurrently while already finished tasks can be added to
 * the index.
 * </p>
 * 
 * 
 * 
 * @author Simon Willnauer
 */
public class GDataIndexer {
    private static final Log LOG = LogFactory.getLog(GDataIndexer.class);

    protected IndexWriter writer;

    protected IndexSearcher searcher;

    protected AtomicInteger committed = new AtomicInteger(0);

    protected AtomicInteger optimized = new AtomicInteger(0);

    private AtomicBoolean isDestroyed = new AtomicBoolean(false);

    protected AtomicInteger docsAdded = new AtomicInteger();

    protected AtomicInteger docsUpdated = new AtomicInteger();

    protected AtomicInteger docsDeleted = new AtomicInteger();

    private final Directory dir;

    private final List<IndexEventListener> listeners = new ArrayList<IndexEventListener>();

    protected final BlockingQueue<Future<IndexDocument>> futurQueue = new LinkedBlockingQueue<Future<IndexDocument>>(
            100);

    private final IndexSchema serviceConfiguration;

    private final ExecutorService indexTaskExecutor;

    protected IndexTask indexTask;

    private static final Integer ZERO = new Integer(0);

    private static final Integer ONE = new Integer(1);

    private final Map<IndexDocument, Integer> action;

    protected GDataIndexer(final IndexSchema schema, Directory dir,
            boolean create) throws IOException {
        if (schema == null)
            throw new IllegalArgumentException(
                    "IndexServiceConfiguration must not be null");
        if (dir == null)
            throw new IllegalArgumentException(
                    "IndexDirectory must not be null");

        this.serviceConfiguration = schema;
        this.dir = dir;
        openWriter(create);
        this.indexTaskExecutor = Executors.newSingleThreadExecutor();
        this.action = new HashMap<IndexDocument, Integer>(128);

    }

    protected void setIndexTask(final IndexTask task) {
        if (task != null && this.indexTask == null)
            this.indexTask = task;
    }

    protected void init() {
        if (this.indexTask == null)
            this.indexTask = new IndexTask(this, this.futurQueue);
        this.indexTaskExecutor.execute(this.indexTask);

    }

    /**
     * Adds the given future task to the queue, and waits if the queue is full.
     * The queue size is set to 100 by default.
     * 
     * @param task -
     *            the task to be scheduled
     * @throws InterruptedException -
     *             if the queue is interrupted
     */
    public void addIndexableDocumentTask(final Future<IndexDocument> task)
            throws InterruptedException {
        if (this.isDestroyed.get())
            throw new IllegalStateException(
                    "Indexer has already been destroyed");
        this.futurQueue.put(task);
    }

    /*
     * a added doc should not be in the index, be sure and delete possible
     * duplicates
     */
    protected synchronized void addDocument(IndexDocument indexable)
            throws IOException {
        if (!indexable.isInsert())
            throw new GdataIndexerException(
                    "Index action must be set to insert");
        setAction(indexable);
        doWrite(indexable);
        this.docsAdded.incrementAndGet();

    }

    private void setAction(IndexDocument doc) {
        Integer docCountToKeep = this.action.get(doc);
        if (!doc.isDelete() && (docCountToKeep == null || docCountToKeep == 0)) {
            /*
             * add a ONE for ONE documents to keep for this IndexDocument when
             * doDelete. doDelete will keep the latest added document and
             * deletes all other documents for this IndexDocument e.g. all
             * duplicates
             */
            this.action.put(doc, ONE);
        } else if (doc.isDelete()
                && (docCountToKeep == null || docCountToKeep > 0)) {
            /*
             * add a zero for zero documents to keep for this IndexDocument when
             * doDelete
             */
            this.action.put(doc, ZERO);
        }
    }

    protected synchronized void updateDocument(IndexDocument indexable)
            throws IOException {
        if (!indexable.isUpdate())
            throw new GdataIndexerException(
                    "Index action must be set to update");
        setAction(indexable);
        doWrite(indexable);
        this.docsUpdated.incrementAndGet();
    }

    protected synchronized void deleteDocument(IndexDocument indexable) {
        if (!indexable.isDelete())
            throw new GdataIndexerException(
                    "Index action must be set to delete");

        setAction(indexable);
        this.docsDeleted.incrementAndGet();
    }

    /**
     * This method commits all changes to the index and closes all open
     * resources (e.g. IndexWriter and IndexReader). This method notifies all
     * registered Commit listeners if invoked.
     * 
     * @param optimize -
     *            <code>true</code> if the index should be optimized on this
     *            commit
     * @throws IOException -
     *             if an IOException occurs
     */
    protected synchronized void commit(boolean optimize) throws IOException {
        if (LOG.isInfoEnabled())
            LOG.info("Commit called with optimize = " + optimize);

        int changes = this.docsAdded.intValue() + this.docsDeleted.intValue()
                + this.docsUpdated.intValue();
        /*
         * don't call listeners to prevent unnecessary close / open of searchers
         */
        if (changes == 0)
            return;
        this.committed.incrementAndGet();
        if(optimize)
            this.optimized.incrementAndGet();
        doDeltete();
        if (optimize) {
            closeSearcher();
            openWriter();
            this.writer.optimize();
        }
        closeSearcher();
        closeWriter();
        this.docsAdded.set(0);
        this.docsDeleted.set(0);
        this.docsUpdated.set(0);
        notifyCommitListeners(this.serviceConfiguration.getName());

    }

    /**
     * Registers a new IndexEventListener. All registered listeners will be
     * notified if the index has been committed.
     * 
     * @param listener -
     *            the listener to register
     * 
     */
    public void registerIndexEventListener(IndexEventListener listener) {
        if (listener == null || this.listeners.contains(listener))
            return;
        this.listeners.add(listener);
    }

    /**
     * Removes a registered IndexEventListener
     * 
     * @param listener -
     *            the listener to remove
     */
    public void removeIndexEventListener(IndexEventListener listener) {

        if (listener == null || !this.listeners.contains(listener))
            return;
        this.listeners.remove(listener);
    }

    protected void notifyCommitListeners(String serviceId) {
        if (LOG.isInfoEnabled())
            LOG.info("notify commit event listeners for service id: "
                    + serviceId + " --  current size of registered listeners: "
                    + this.listeners.size());
        for (IndexEventListener listener : this.listeners) {
            listener.commitCallBack(serviceId);
        }
    }

    protected void closeWriter() throws IOException {
        try {
            if (this.writer != null)
                this.writer.close();
        } finally {
            this.writer = null;
        }
    }

    protected void closeSearcher() throws IOException {
        try {
            if (this.searcher != null)
                this.searcher.close();
        } finally {
            this.searcher = null;
        }
    }

    protected void openSearcher() throws IOException {
        if (this.searcher == null)
            this.searcher = new IndexSearcher(this.dir);
    }

    protected void openWriter() throws IOException {
        openWriter(false);
    }

    private void openWriter(boolean create) throws IOException {
        if (this.writer == null)
            this.writer = new GDataIndexWriter(this.dir, create,
                    this.serviceConfiguration);
    }

    /*
     * This should only be called in a synchronized block
     */
    protected void doWrite(IndexDocument document) throws IOException {
        closeSearcher();
        openWriter();
        this.writer.addDocument(document.getWriteable());

    }

    // only access synchronized
    int[] documentNumber;

    /*
     * This should only be called in a synchronized block
     */
    protected void doDeltete() throws IOException {
        if (this.action.size() == 0)
            return;
        if (LOG.isInfoEnabled())
            LOG
                    .info("Deleting documents and duplicates from index, size of IndexDocuments "
                            + this.action.size());
        closeWriter();
        openSearcher();

        IndexReader reader = this.searcher.getIndexReader();
        TermDocs termDocs = reader.termDocs();
        for (Map.Entry<IndexDocument, Integer> entry : this.action.entrySet()) {
            IndexDocument indexDocument = entry.getKey();
            Integer docToKeep = entry.getValue();
            // extend the array if needed
            if (this.documentNumber == null
                    || docToKeep > this.documentNumber.length)
                this.documentNumber = new int[docToKeep];

            for (int i = 0; i < this.documentNumber.length; i++) {

                this.documentNumber[i] = -1;
            }
            /*
             * get the term to find the document from the document itself
             */
            termDocs.seek(indexDocument.getDeletealbe());

            int pos = 0;

            while (termDocs.next()) {
                /*
                 * if this is a pure delete just delete it an continue
                 */
                if (docToKeep == 0) {
                    reader.deleteDocument(termDocs.doc());
                    continue;
                }

                int prev = this.documentNumber[pos];
                this.documentNumber[pos] = termDocs.doc();
                if (prev != -1) {
                    reader.deleteDocument(prev);
                }

                if (++pos >= docToKeep)
                    pos = 0;

            }
        }
        /*
         * clear the map after all documents are processed
         */
        this.action.clear();
        closeSearcher();
    }

    protected synchronized void destroy() throws IOException {
        this.isDestroyed.set(true);
        if (!this.indexTask.isStopped())
            this.indexTask.stop();
        this.futurQueue.add(new FinishingFuture());
        this.indexTaskExecutor.shutdown();
        closeWriter();
        closeSearcher();
        if (LOG.isInfoEnabled())
            LOG.info("Destroying GdataIndexer for service -- "
                    + this.serviceConfiguration.getName());

    }

    /**
     * This factory method creates a new GDataIndexer using a instance of
     * {@link IndexTask}
     * 
     * @param config -
     *            the config to be used to configure the indexer
     * @param dir -
     *            the directory to index to
     * @param create -
     *            <code>true</code> to create a new index, <code>false</code>
     *            to use the existing one.
     * @return - a new GDataIndexer instance
     * @throws IOException -
     *             if an IOException occurs while initializing the indexer
     */
    public static synchronized GDataIndexer createGdataIndexer(
            final IndexSchema config, Directory dir, boolean create)
            throws IOException {
        GDataIndexer retVal = new GDataIndexer(config, dir, create);
        retVal.setIndexTask(new IndexTask(retVal, retVal.futurQueue));
        retVal.init();
        return retVal;
    }

    /**
     * This factory method creates a new GDataIndexer using a instance of
     * {@link TimedIndexTask}. This indexer will automatically commit the index
     * if no modification to the index occur for the given time. The used time
     * unit is {@link TimeUnit#SECONDS}. Values less than the default value
     * will be ignored. For the default value see {@link TimedIndexTask}.
     * 
     * @param config -
     *            the config to be used to configure the indexer
     * @param dir -
     *            the directory to index to
     * @param create -
     *            <code>true</code> to create a new index, <code>false</code>
     *            to use the existing one.
     * @param commitTimeout -
     *            the amount of seconds to wait until a commit should be
     *            scheduled
     * @return - a new GDataIndexer instance
     * @throws IOException -
     *             if an IOException occurs while initializing the indexer
     */
    public static synchronized GDataIndexer createTimedGdataIndexer(
            final IndexSchema config, Directory dir, boolean create,
            long commitTimeout) throws IOException {

        GDataIndexer retVal = new GDataIndexer(config, dir, create);
        retVal.setIndexTask(new TimedIndexTask(retVal, retVal.futurQueue,
                commitTimeout));
        retVal.init();
        return retVal;
    }

    @SuppressWarnings("unused")
    static final class FinishingFuture implements Future<IndexDocument> {

        /**
         * @see java.util.concurrent.Future#cancel(boolean)
         */
        public boolean cancel(boolean arg0) {

            return false;
        }

        /**
         * @see java.util.concurrent.Future#isCancelled()
         */
        public boolean isCancelled() {

            return false;
        }

        /**
         * @see java.util.concurrent.Future#isDone()
         */
        public boolean isDone() {

            return true;
        }

        /**
         * @see java.util.concurrent.Future#get()
         */
        @SuppressWarnings("unused")
        public IndexDocument get() throws InterruptedException,
                ExecutionException {

            return null;
        }

        /**
         * @see java.util.concurrent.Future#get(long,
         *      java.util.concurrent.TimeUnit)
         */
        @SuppressWarnings("unused")
        public IndexDocument get(long arg0, TimeUnit arg1)
                throws InterruptedException, ExecutionException,
                TimeoutException {

            return null;
        }

    }

}
