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
package org.apache.lucene.gdata.search.config;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.gdata.search.index.IndexDocument;
import org.apache.lucene.gdata.utils.ReflectionUtils;

/**
 * This class is used to configure the indexing and search component. Each
 * service on the GData server will have an own search index. For this purpose
 * one single index schema will be configured in the gdata-config.xml file. This
 * file will be mapped on this class on startup.
 * <p>
 * This class breaks some encapsulation of general java classes to be
 * configurable via the xml configuration file. The will be very less type and
 * value checking of the properties inside this file. Mandatory values must be
 * set in the configuration file. The server won't start up if these values are
 * missing. See definition in the xml schema file. If this class is instantiated
 * manually the value for the name of the schema should be set before this is
 * passed to the IndexController.
 * </p>
 * <p>
 * One IndexSchema consists of multiple instances of
 * {@link org.apache.lucene.gdata.search.config.IndexSchemaField} each of this
 * instances describes a single field in the index and all schema informations
 * about the field.
 * <p>
 * 
 * 
 * @see org.apache.lucene.gdata.search.config.IndexSchemaField
 * 
 * 
 * @author Simon Willnauer
 */
public class IndexSchema {
    private final Set<String> searchableFieldNames = new HashSet<String>();

    private static final Log LOG = LogFactory.getLog(IndexSchema.class);

    /**
     * a static final value for properties are not set by the configuration file
     * this value will be set to all long and int properties by default
     */
    public static final int NOT_SET_VALUE = -1;
    private static final int DEFAULT_OPTIMIZE_COUNT = 1;
    private static final int DEFAULT_COMMIT_COUNT = 1;

    private String indexLocation;

    /*
     * this should be final change it if possible --> see commons digester /
     * RegistryBuilder
     */
    private String name;

    private boolean useTimedIndexer;

    private long indexerIdleTime = NOT_SET_VALUE;

    private Analyzer serviceAnalyzer;

    private String defaultSearchField;

    private PerFieldAnalyzerWrapper perFieldAnalyzer;

    private Collection<IndexSchemaField> schemaFields;

    private int maxBufferedDocs = NOT_SET_VALUE;

    private int maxMergeDocs = NOT_SET_VALUE;

    private int mergeFactor = NOT_SET_VALUE;

    private int maxFieldLength = NOT_SET_VALUE;

    private long writeLockTimeout = NOT_SET_VALUE;

    private long commitLockTimeout = NOT_SET_VALUE;

    private int commitAfterDocuments = DEFAULT_COMMIT_COUNT;
    
    private int optimizeAfterCommit = DEFAULT_OPTIMIZE_COUNT;
    
    private boolean useCompoundFile = false;

    /**
     * Creates a new IndexSchema and initialize the standard service analyzer to
     * {@link StandardAnalyzer}
     * 
     */
    public IndexSchema() {
        this.schemaFields = new ArrayList<IndexSchemaField>();
        /*
         * keep as standard if omitted in the configuration
         */
        this.serviceAnalyzer = new StandardAnalyzer();

    }

    /**
     * Initialize the schema and checks all required values
     */
    public void initialize() {
        for (IndexSchemaField field : this.schemaFields) {
            if (!field.checkRequieredValues())
                throw new RuntimeException("Required Value for field: "
                        + field.getName() + " is missing");
        }
        if (this.defaultSearchField == null)
            throw new RuntimeException("DefaulSearchField must not be null");
        if (this.name == null)
            throw new RuntimeException(
                    "Schema field is not set -- must not be null");
        if (this.indexLocation == null)
            throw new RuntimeException("IndexLocation must not be null");
        if(!this.searchableFieldNames.contains(this.defaultSearchField)){
            throw new RuntimeException("the default search field: "+this.defaultSearchField+" is registered as a field");
        }

    }

    /**
     * @return Returns the useCompoundFile.
     */
    public boolean isUseCompoundFile() {
        return this.useCompoundFile;
    }

    /**
     * @param useCompoundFile
     *            The useCompoundFile to set.
     */
    public void setUseCompoundFile(boolean useCompoundFile) {
        this.useCompoundFile = useCompoundFile;
    }

    /**
     * Adds a new {@link IndexSchemaField} to the schema. if the fields name
     * equals {@link IndexDocument#FIELD_ENTRY_ID} or the field is
     * <code>null</code> it will simply ignored
     * 
     * @param field -
     *            the index schema field to add as a field of this schema.
     */
    public void addSchemaField(final IndexSchemaField field) {
        if (field == null)
            return;
        /*
         * skip fields configured in the gdata-config.xml file if their names
         * match a primary key field id of the IndexDocument
         */
        if (field.getName().equals(IndexDocument.FIELD_ENTRY_ID)
                || field.getName().equals(IndexDocument.FIELD_FEED_ID))
            return;
        if (field.getAnalyzerClass() != null) {
            /*
             * enable per field analyzer if one is set.
             */
            Analyzer analyzer = getAnalyzerInstance(field.getAnalyzerClass());
            /*
             * null values will be omitted here
             */
            buildPerFieldAnalyzerWrapper(analyzer, field.getName());
        }
        this.schemaFields.add(field);
        this.searchableFieldNames.add(field.getName());
    }


    /**
     * @return Returns the fieldConfiguration.
     */
    public Collection<IndexSchemaField> getFields() {
        return this.schemaFields;
    }

    /**
     * @return - the analyzer instance to be used for this schema
     */
    public Analyzer getSchemaAnalyzer() {
        if (this.perFieldAnalyzer == null)
            return this.serviceAnalyzer;
        return this.perFieldAnalyzer;
    }

    /**
     * @return Returns the serviceAnalyzer.
     */
    public Analyzer getServiceAnalyzer() {
        return this.serviceAnalyzer;
    }

    /**
     * @param serviceAnalyzer
     *            The serviceAnalyzer to set.
     */
    public void setServiceAnalyzer(Analyzer serviceAnalyzer) {
        if (serviceAnalyzer == null)
            return;
        this.serviceAnalyzer = serviceAnalyzer;

    }

    /**
     * @return Returns the commitLockTimout.
     */
    public long getCommitLockTimeout() {
        return this.commitLockTimeout;
    }

    /**
     * 
     * @param commitLockTimeout
     *            The commitLockTimeout to set.
     */
    public void setCommitLockTimeout(long commitLockTimeout) {
        // TODO enable this in config
        this.commitLockTimeout = commitLockTimeout;
    }

    /**
     * @return Returns the maxBufferedDocs.
     */
    public int getMaxBufferedDocs() {

        return this.maxBufferedDocs;
    }

    /**
     * @param maxBufferedDocs
     *            The maxBufferedDocs to set.
     */
    public void setMaxBufferedDocs(int maxBufferedDocs) {
        this.maxBufferedDocs = maxBufferedDocs;
    }

    /**
     * @return Returns the maxFieldLength.
     */
    public int getMaxFieldLength() {
        return this.maxFieldLength;
    }

    /**
     * @param maxFieldLength
     *            The maxFieldLength to set.
     */
    public void setMaxFieldLength(int maxFieldLength) {
        this.maxFieldLength = maxFieldLength;
    }

    /**
     * @return Returns the maxMergeDocs.
     */
    public int getMaxMergeDocs() {
        return this.maxMergeDocs;
    }

    /**
     * @param maxMergeDocs
     *            The maxMergeDocs to set.
     */
    public void setMaxMergeDocs(int maxMergeDocs) {
        this.maxMergeDocs = maxMergeDocs;
    }

    /**
     * @return Returns the mergeFactor.
     */
    public int getMergeFactor() {
        return this.mergeFactor;
    }

    /**
     * @param mergeFactor
     *            The mergeFactor to set.
     */
    public void setMergeFactor(int mergeFactor) {
        this.mergeFactor = mergeFactor;
    }

    /**
     * @return Returns the writeLockTimeout.
     */
    public long getWriteLockTimeout() {
        return this.writeLockTimeout;
    }

    /**
     * @param writeLockTimeout
     *            The writeLockTimeout to set.
     */
    public void setWriteLockTimeout(long writeLockTimeout) {
        this.writeLockTimeout = writeLockTimeout;
    }

    /**
     * @param fields
     *            The fieldConfiguration to set.
     */
    public void setSchemaFields(Collection<IndexSchemaField> fields) {
        this.schemaFields = fields;
    }

    /**
     * @return Returns the name.
     */
    public String getName() {
        return this.name;
    }

    /**
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object object) {
        if (this == object)
            return true;
        if (object == null)
            return false;
        if (object instanceof IndexSchema) {
           if(this.name ==null)
               return super.equals(object);
            return this.name.equals(((IndexSchema) object).getName());
        }
        return false;
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        if (this.name == null)
            return super.hashCode();
        return this.name.hashCode();
    }

    private void buildPerFieldAnalyzerWrapper(Analyzer anazlyer, String field) {
        if (anazlyer == null || field == null || field.length() == 0)
            return;
        if (this.perFieldAnalyzer == null)
            this.perFieldAnalyzer = new PerFieldAnalyzerWrapper(
                    this.serviceAnalyzer);
        this.perFieldAnalyzer.addAnalyzer(field, anazlyer);
    }

    private static Analyzer getAnalyzerInstance(Class<? extends Analyzer> clazz) {
        if (!ReflectionUtils.extendsType(clazz, Analyzer.class)) {
            LOG.warn("Can not create analyzer for class " + clazz.getName());
            return null;
        }
        try {
            return clazz.newInstance();
        } catch (Exception e) {
            LOG.warn("Can not create analyzer for class " + clazz.getName());
        }
        return null;
    }

    /**
     * @param name
     *            The name to set.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return Returns the indexLocation.
     */
    public String getIndexLocation() {
        return this.indexLocation;
    }

    /**
     * @param indexLocation
     *            The indexLocation to set.
     */
    public void setIndexLocation(String indexLocation) {
        this.indexLocation = indexLocation;
    }

    /**
     * @return Returns the defaultField.
     */
    public String getDefaultSearchField() {
        return this.defaultSearchField;
    }

    /**
     * @param defaultField
     *            The defaultField to set.
     */
    public void setDefaultSearchField(String defaultField) {
        this.defaultSearchField = defaultField;
    }

    /**
     * @return Returns the indexerIdleTime.
     */
    public long getIndexerIdleTime() {
        return this.indexerIdleTime;
    }

    /**
     * @param indexerIdleTime
     *            The indexerIdleTime to set.
     */
    public void setIndexerIdleTime(long indexerIdleTime) {
        this.indexerIdleTime = indexerIdleTime;
    }

    /**
     * @return Returns the useTimedIndexer.
     */
    public boolean isUseTimedIndexer() {
        return this.useTimedIndexer;
    }

    /**
     * @param useTimedIndexer
     *            The useTimedIndexer to set.
     */
    public void setUseTimedIndexer(boolean useTimedIndexer) {
        this.useTimedIndexer = useTimedIndexer;
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(this.getClass().getName())
                .append(" ");
        builder.append("Name: ").append(this.name).append(" ");
        builder.append("MaxBufferedDocs: ").append(this.maxBufferedDocs)
                .append(" ");
        builder.append("MaxFieldLength: ").append(this.maxFieldLength).append(
                " ");
        builder.append("MaxMergeDocs: ").append(this.maxMergeDocs).append(" ");
        builder.append("MergeFactor: ").append(this.mergeFactor).append(" ");
        builder.append("CommitLockTimeout: ").append(this.commitLockTimeout)
                .append(" ");
        builder.append("WriteLockTimeout: ").append(this.writeLockTimeout)
                .append(" ");
        builder.append("indexerIdleTime: ").append(this.indexerIdleTime)
                .append(" ");
        builder.append("useCompoundFile: ").append(this.useCompoundFile)
                .append(" ");
        builder.append("Added SchemaField instances: ").append(
                this.schemaFields.size()).append(" ");

        builder.append("IndexLocation: ").append(this.indexLocation)
                .append(" ");
        return builder.toString();

    }

    /**
     * @return Returns the searchableFieldNames.
     */
    public Set<String> getSearchableFieldNames() {
        return this.searchableFieldNames;
    }

    /**
     * Defines after how many added,removed or updated document the indexer should commit.
     * @return Returns the commitAfterDocuments.
     */
    public int getCommitAfterDocuments() {
        return this.commitAfterDocuments;
    }

    /**
     * @param commitAfterDocuments The commitAfterDocuments to set.
     */
    public void setCommitAfterDocuments(int commitAfterDocuments) {
        if(commitAfterDocuments < DEFAULT_COMMIT_COUNT)
            return;
        this.commitAfterDocuments = commitAfterDocuments;
    }

    /**
     * Defines after how many commits the indexer should optimize the index
     * @return Returns the optimizeAfterCommit.
     */
    public int getOptimizeAfterCommit() {
        
        return this.optimizeAfterCommit;
    }

    /**
     * @param optimizeAfterCommit The optimizeAfterCommit to set.
     */
    public void setOptimizeAfterCommit(int optimizeAfterCommit) {
        if(optimizeAfterCommit < DEFAULT_OPTIMIZE_COUNT )
            return;
        this.optimizeAfterCommit = optimizeAfterCommit;
    }
}
