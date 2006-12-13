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

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.gdata.data.ServerBaseEntry;
import org.apache.lucene.gdata.data.ServerBaseFeed;
import org.apache.lucene.gdata.search.GDataSearcher;
import org.apache.lucene.gdata.search.SearchComponent;
import org.apache.lucene.gdata.search.StandardGdataSearcher;
import org.apache.lucene.gdata.search.config.IndexSchema;
import org.apache.lucene.gdata.server.registry.Component;
import org.apache.lucene.gdata.server.registry.ComponentType;
import org.apache.lucene.gdata.server.registry.EntryEventListener;
import org.apache.lucene.gdata.server.registry.GDataServerRegistry;
import org.apache.lucene.gdata.server.registry.ProvidedService;
import org.apache.lucene.gdata.utils.ReferenceCounter;
import org.apache.lucene.index.IndexFileNameFilter;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

/**
 * Default implementation of the {@link SearchComponent} interface. All actions
 * on the index will be controlled from this class. Only this class grants read
 * or write actions access to the index.
 * 
 * @author Simon Willnauer
 * 
 */
@Component(componentType = ComponentType.SEARCHCONTROLLER)
public class IndexController implements SearchComponent, IndexEventListener,
        EntryEventListener {
    static final Log LOG = LogFactory.getLog(IndexController.class);

    private final AtomicBoolean isInitialized = new AtomicBoolean(false);

    private final AtomicBoolean destroyed = new AtomicBoolean(false);

    protected Map<String, ServiceIndex> indexerMap;

    private final ExecutorService taskExecutor;

    /**
     * Creates a new IndexController -- call
     * {@link IndexController#initialize()} to set up the controller.
     */
    public IndexController() {
        this.taskExecutor = Executors.newCachedThreadPool();
    }

    /**
     * @see org.apache.lucene.gdata.search.SearchComponent#initialize()
     */
    public synchronized void initialize() {
        if (this.isInitialized.get())
            throw new IllegalStateException(
                    "IndexController is already initialized");
        this.destroyed.set(false);
        /*
         * if this fails the server must not startup --> throw runtime exception
         */
        GDataServerRegistry.getRegistry().registerEntryEventListener(this);

        GDataServerRegistry.getRegistry().registerEntryEventListener(this);
        Collection<ProvidedService> services = GDataServerRegistry
                .getRegistry().getServices();
        this.indexerMap = new ConcurrentHashMap<String, ServiceIndex>(services
                .size());
       
        for (ProvidedService service : services) {
            IndexSchema schema = service.getIndexSchema();
            /*
             * initialize will fail if mandatory values are not set. This is
             * just a
             */
            schema.initialize();
            addIndexSchema(schema);
        }
        this.isInitialized.set(true);
        

    }

    /*
     * add a schema to the index controller and create the indexer. create
     * directories and check out existing indexes
     */
    protected void addIndexSchema(final IndexSchema schema) {
        checkDestroyed();
        if (schema.getName() == null)
            throw new IllegalStateException(
                    "schema has no name -- is not associated with any service");
        if (this.indexerMap.containsKey(schema.getName()))
            throw new IllegalStateException("schema for service "
                    + schema.getName() + " is already registered");
        if (LOG.isInfoEnabled())
            LOG.info("add new IndexSchema for service " + schema.getName()
                    + " -- " + schema);
        try {
            ServiceIndex bean = createIndexer(schema);
            ReferenceCounter<IndexSearcher> searcher = getNewServiceSearcher(bean.getDirectory());
            bean.setSearcher(searcher);
            this.indexerMap.put(schema.getName(), bean);
        } catch (IOException e) {
            LOG.error("Can not create indexer for service " + schema.getName(),
                    e);
            throw new GdataIndexerException(
                    "Can not create indexer for service " + schema.getName(), e);
        }

    }

    protected ServiceIndex createIndexer(final IndexSchema schema) throws IOException {
        GDataIndexer indexer;
        File indexLocation = createIndexLocation(schema.getIndexLocation(),
                schema.getName());
        boolean create = createIndexDirectory(indexLocation);
        Directory dir = FSDirectory.getDirectory(indexLocation, create);
        if (LOG.isInfoEnabled())
            LOG.info("Create new Indexer for IndexSchema: " + schema);
        /*
         * timed or committed indexer?! keep the possibility to let users decide
         * to use scheduled commits
         */
        if (schema.isUseTimedIndexer())
            indexer = GDataIndexer.createTimedGdataIndexer(schema, dir, create,
                    schema.getIndexerIdleTime());
        else
            indexer = GDataIndexer.createGdataIndexer(schema, dir, create);
        indexer.registerIndexEventListener(this);
        return new ServiceIndex(schema, indexer, dir);
    }

    /*
     * if this fails the server must not startup!!
     */
    protected File createIndexLocation(final String path,final  String name) {
        if (path == null || name == null)
            throw new GdataIndexerException(
                    "Path or Name of the index location is not set Path: "
                            + path + " name: " + name);
        /*
         * check if parent e.g. the configured path is a directory
         */
        File parent = new File(path);
        if (!parent.isDirectory())
            throw new IllegalArgumentException(
                    "the given path is not a directory -- " + path);
        /*
         * try to create and throw ex if fail
         */
        if (!parent.exists())
            if (!parent.mkdir())
                throw new RuntimeException("Can not create directory -- "
                        + path);
        /*
         * try to create and throw ex if fail
         */
        File file = new File(parent, name);
        if (file.isFile())
            throw new IllegalArgumentException(
                    "A file with the name"
                            + name
                            + " already exists in "
                            + path
                            + " -- a file of the name of the service must not exist in the index location");

        if (!file.exists()) {
            if (!file.mkdir())
                throw new RuntimeException("Can not create directory -- "
                        + file.getAbsolutePath());
        }
        return file;
    }

    protected boolean createIndexDirectory(final File file) {
        /*
         * use a lucene filename filter to figure out if there is an existing
         * index in the defined directory
         */
        String[] luceneFiles = file.list(new IndexFileNameFilter());
        return !(luceneFiles.length > 0);

    }

    /**
     * @see org.apache.lucene.gdata.search.index.IndexEventListener#commitCallBack(java.lang.String)
     */
    public synchronized void commitCallBack(final String service) {
        checkDestroyed();
        if(LOG.isInfoEnabled())
            LOG.info("CommitCallback triggered - register new searcher for service: "+service);
        /*
         * get the old searcher and replace it if possible.
         */
        ServiceIndex index = this.indexerMap.get(service);
        ReferenceCounter<IndexSearcher> searcher = index.getSearcher();

        try {
            index.setSearcher(getNewServiceSearcher(index.getDirectory()));
        } catch (IOException e) {
            LOG.fatal("Can not create new Searcher -- keep the old one ", e);
            return;
        }
        /*
         * if new searcher if registered decrement old one to get it destroyed if unused
         */
        searcher.decrementRef();
    }
    /*
     * create a new ReferenceCounter for the indexSearcher.
     * The reference is already incremented before returned
     */
    private ReferenceCounter<IndexSearcher> getNewServiceSearcher(final Directory dir)
            throws IOException {
        if(LOG.isInfoEnabled())
            LOG.info("Create new ServiceSearcher");
        IndexSearcher searcher = new IndexSearcher(dir);
        ReferenceCounter<IndexSearcher> holder = new ReferenceCounter<IndexSearcher>(
                searcher) {

            @Override
            protected void close() {
                try {
                    LOG
                            .info("Close IndexSearcher -- Zero references remaining");
                    this.resource.close();
                } catch (IOException e) {
                    LOG.warn("Can not close IndexSearcher -- ", e);
                }
            }

        };
        holder.increamentReference();
        return holder;
    }

    /**
     * @see org.apache.lucene.gdata.server.registry.EntryEventListener#fireUpdateEvent(org.apache.lucene.gdata.data.ServerBaseEntry)
     */
    public void fireUpdateEvent(final ServerBaseEntry entry) {
        createNewIndexerTask(entry, IndexAction.UPDATE);
    }

    /**
     * @see org.apache.lucene.gdata.server.registry.EntryEventListener#fireInsertEvent(org.apache.lucene.gdata.data.ServerBaseEntry)
     */
    public void fireInsertEvent(final ServerBaseEntry entry) {
        createNewIndexerTask(entry, IndexAction.INSERT);
    }

    /**
     * @see org.apache.lucene.gdata.server.registry.EntryEventListener#fireDeleteEvent(org.apache.lucene.gdata.data.ServerBaseEntry)
     */
    public void fireDeleteEvent(final ServerBaseEntry entry) {
        createNewIndexerTask(entry, IndexAction.DELETE);

    }
    
    /**
     * @see org.apache.lucene.gdata.server.registry.EntryEventListener#fireDeleteAllEntries(org.apache.lucene.gdata.data.ServerBaseFeed)
     */
    public void fireDeleteAllEntries(final ServerBaseFeed feed) {
        createNewDeleteAllEntriesTask(feed);
    }
    
    private void createNewDeleteAllEntriesTask(final ServerBaseFeed feed){
        checkDestroyed();
        checkInitialized();
        if(LOG.isInfoEnabled())
            LOG.info("Deleting all entries for feed dispatch new IndexDocumentBuilder -- "+feed.getId());
        String serviceName = feed.getServiceConfig().getName();
        ServiceIndex bean = this.indexerMap.get(serviceName);
        if (bean == null)
            throw new RuntimeException("no indexer for service " + serviceName
                    + " registered");
        Lock lock = bean.getLock();
        lock.lock();
        try{
            IndexDocumentBuilder<IndexDocument> callable = new IndexFeedDeleteTask(feed.getId());
            sumbitTask(callable,bean.getIndexer());
        }finally{
            lock.unlock();
        }
            
        
    }

    // TODO add test for this method!!
    private void createNewIndexerTask(final ServerBaseEntry entry, final IndexAction action) {
        checkDestroyed();
        checkInitialized();
        String serviceName = entry.getServiceConfig().getName();
        if (LOG.isInfoEnabled())
            LOG.info("New Indexer Task submitted - Action: " + action
                    + " for service: " + serviceName);
        ServiceIndex bean = this.indexerMap.get(serviceName);
        if (bean == null)
            throw new RuntimeException("no indexer for service " + serviceName
                    + " registered");
        /*
         * lock on service to synchronize the event order. This lock has
         * fairness parameter set to true. Grant access to the longest waiting
         * thread. Using fairness is slower but is acceptable in this context
         */
        Lock lock = bean.getLock();
        lock.lock();
        try {
            IndexSchema schema = bean.getSchema();
            boolean commitAfter = bean.incrementActionAndReset(schema.getCommitAfterDocuments());
            IndexDocumentBuilder<IndexDocument> callable = new IndexDocumentBuilderTask<IndexDocument>(
                    entry, bean.getSchema(), action, commitAfter,bean.getOptimize(schema.getOptimizeAfterCommit()));
            sumbitTask(callable,bean.getIndexer());
        } finally {
            /*
             * make sure to unlock
             */
            lock.unlock();
        }

    }

    private void sumbitTask(final Callable<IndexDocument> callable, final GDataIndexer indexer){
        Future<IndexDocument> task = this.taskExecutor.submit(callable);
        try {
            indexer.addIndexableDocumentTask(task);
        } catch (InterruptedException e) {
            throw new GdataIndexerException(
                    "Can not accept any index tasks -- interrupted. ", e);

        }
    }    

    /**
     * @see org.apache.lucene.gdata.search.SearchComponent#getServiceSearcher(org.apache.lucene.gdata.server.registry.ProvidedService)
     */
    public GDataSearcher<String> getServiceSearcher(final ProvidedService service) {
        checkDestroyed();
        checkInitialized();

        /*
         * get and increment. searcher will be decremented if GdataSearcher is
         * closed
         */
        ReferenceCounter<IndexSearcher> searcher;
        synchronized (this) {
            ServiceIndex serviceIndex = this.indexerMap.get(service.getName());
            if(serviceIndex == null)
                throw new RuntimeException("no index for service "+service.getName());
            searcher = serviceIndex.getSearcher();
            searcher.increamentReference();
        }

        return new StandardGdataSearcher(searcher);
    }

    /**
     * @see org.apache.lucene.gdata.search.SearchComponent#destroy()
     */
    public synchronized void destroy() {
        checkDestroyed();
        if(!this.isInitialized.get())
            return;
        this.destroyed.set(true);
        this.isInitialized.set(false);
        LOG.info("Shutting down IndexController -- destroy has been called");
        Set<Entry<String, ServiceIndex>> entrySet = this.indexerMap.entrySet();
        for (Entry<String, ServiceIndex> entry : entrySet) {
            ServiceIndex bean = entry.getValue();
            bean.getSearcher().decrementRef();
            GDataIndexer indexer = bean.getIndexer();
            try {
                indexer.destroy();
            } catch (IOException e) {
                LOG.warn("Can not destroy indexer for service: "
                        + bean.getSchema().getName(), e);
            }
        }
        this.taskExecutor.shutdown();
        this.indexerMap.clear();
    }

    private void checkDestroyed(){
        if (this.destroyed.get())
            throw new IllegalStateException(
                    "IndexController has been destroyed");   
    }
    private void checkInitialized(){
        if(!this.isInitialized.get())
            throw new IllegalStateException(
            "IndexController has not been initialized");
    }   
    
    
    final static class ServiceIndex {
        private AtomicInteger actionCount = new AtomicInteger(0);
        
        private AtomicInteger commitCount = new AtomicInteger(0);
        
        private final Lock lock;

        private final IndexSchema schema;

        private final GDataIndexer indexer;

        private final Directory directory;
        
        private Filter addedDocumentFilter;
        
        private ReferenceCounter<IndexSearcher> searcher;

        // private final Map<String,IndexAction> actionMap;

       

        ServiceIndex(final IndexSchema schema, GDataIndexer indexer,
                Directory directory) {
            this.schema = schema;
            this.indexer = indexer;
            this.lock = new ReentrantLock(true);
            this.directory = directory;
            // this.actionMap = new HashMap<String,IndexAction>(128);
        }

        Lock getLock() {
            return this.lock;
        }

        /**
         * @return Returns the indexer.
         */
        GDataIndexer getIndexer() {
            return this.indexer;
        }

        /**
         * @return Returns the schema.
         */
        IndexSchema getSchema() {
            return this.schema;
        }

        // public void addAction(IndexAction action,ServerBaseEntry entry){
        //            
        // }
        /**
         * Counts how many actions have been executed on this index
         * 
         * @param reset - count mod reset value equals 0 causes a commit
         *            
         * @return <code>true</code> if the count mod reset value equals 0, otherwise
         *         false;
         */
        boolean incrementActionAndReset(int reset) {
            if (this.actionCount.incrementAndGet()%reset == 0) {
                return true;
            }
            return false;
        }

        /**
         * @return Returns the directory.
         */
        public Directory getDirectory() {
            return this.directory;
        }
        /**
         * @return Returns the addedDocumentFilter.
         */
        public Filter getAddedDocumentFilter() {
            return this.addedDocumentFilter;
        }

        /**
         * @param addedDocumentFilter The addedDocumentFilter to set.
         */
        public void setAddedDocumentFilter(Filter addedDocumentFilter) {
            this.addedDocumentFilter = addedDocumentFilter;
        }

        /**
         * @return Returns the searcher.
         */
        public ReferenceCounter<IndexSearcher> getSearcher() {
            return this.searcher;
        }

        /**
         * @param searcher The searcher to set.
         */
        public void setSearcher(ReferenceCounter<IndexSearcher> searcher) {
            this.searcher = searcher;
        }

        /**
         * @return Returns the commitCount.
         */
        public int commitCountIncrement() {
            return this.commitCount.incrementAndGet();
        }
        /**
         * @param reset - the number after how many commits the index should be optimized
         * @return <code>true</code> if and only if the commit count mod reset equals 0, otherwise <code>false</code>.
         */
        public boolean getOptimize(int reset){
            if(this.commitCount.get()%reset == 0){
                return true;
            }
            return false;
        }
    }


}
