/** 
 * Copyright 2004 The Apache Software Foundation 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
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

import java.io.IOException;
import java.io.StringWriter;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.gdata.data.ServerBaseEntry;
import org.apache.lucene.gdata.server.registry.ProvidedService;
import org.apache.lucene.gdata.server.registry.ProvidedServiceConfig;
import org.apache.lucene.gdata.storage.lucenestorage.StorageBuffer.BufferableEntry;

import com.google.gdata.data.BaseEntry;
import com.google.gdata.util.common.xml.XmlWriter;

/**
 * This immutable class wrapps <tt>ServerBaseEntry</tt> for an internal
 * Storage representation of an entry. This class also acts as a Documentfactory
 * for lucene documents to be stored inside the index.
 * 
 * @author Simon Willnauer
 * 
 */
public class StorageEntryWrapper implements Comparable<StorageEntryWrapper>,
        StorageWrapper {

    private static final long serialVersionUID = -4619985652059888526L;

    private static final String INTERNAL_ENCODING = "UTF-8";

    /**
     * lucene field name Entry id
     */
    public final static String FIELD_ENTRY_ID = "entryId";

    /**
     * lucene field name feed id
     */
    public final static String FIELD_FEED_REFERENCE = "feedReference";

    /**
     * lucene field name entry content
     */
    public final static String FIELD_CONTENT = "content";

    /**
     * lucene field name creating timestamp
     */
    public final static String FIELD_TIMESTAMP = "timestamp";

    private final String entryId;

    private final String feedId;

    private  String content;

    private final ServerBaseEntry entry;

    private Long timestamp;

    private transient Document document;

    private StorageOperation operation;

    private ProvidedService config;

    /**
     * Creates a new StorageEntryWrapper.
     * 
     * @param entry -
     *            the entry to wrap
     * 
     * @param operation -
     *            the StorageOperation
     * 
     * @throws IOException -
     *             if the entry content can not be generated
     */
    public StorageEntryWrapper(final ServerBaseEntry entry,
            StorageOperation operation) throws IOException {

        this.entry = entry;
        this.operation = operation;
        this.entryId = entry.getId();
        this.feedId = entry.getFeedId();
        if (operation != StorageOperation.DELETE) {
            this.config = entry.getServiceConfig();
            this.content = buildContent();
         
        }
        this.timestamp = new Long(
                this.entry.getUpdated() != null ? this.entry.getUpdated()
                        .getValue() : System.currentTimeMillis());
            

    }

    private String buildContent() throws IOException {
        StringWriter writer = new StringWriter();
        XmlWriter xmlWriter = new XmlWriter(writer, INTERNAL_ENCODING);
        this.entry.generateAtom(xmlWriter, this.config.getExtensionProfile());
        return writer.toString();

    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageWrapper#getLuceneDocument()
     */
    public Document getLuceneDocument() {
        if(this.operation == StorageOperation.DELETE)
            return null;
        if (this.document != null)
            return this.document;
        this.document = new Document();
        this.document.add(new Field(FIELD_ENTRY_ID, this.entryId,
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        this.document.add(new Field(FIELD_FEED_REFERENCE, this.feedId,
                Field.Store.YES, Field.Index.UN_TOKENIZED));
        this.document.add(new Field(FIELD_CONTENT, this.content,
                Field.Store.COMPRESS, Field.Index.NO));
        this.document.add(new Field(FIELD_TIMESTAMP, this.timestamp.toString(),
                Field.Store.YES, Field.Index.UN_TOKENIZED));

        return this.document;

    }

    /**
     * @return - the wrapped entry
     */
    public BaseEntry getEntry() {
        /*
         * this wrapps the entry again. BufferableEntry creates a new instance
         * for the dynamic element like links.
         */
        return new BufferableEntry(this.entry.getEntry());
    }

    /**
     * @return - the entry id
     */
    public String getEntryId() {
        return this.entryId;
    }

    /**
     * @return - the feed id
     */
    public String getFeedId() {
        return this.feedId;
    }

    /**
     * Storage operations
     * 
     * @author Simon Willnauer
     * 
     */
    public static enum StorageOperation {
        /**
         * delete
         */
        DELETE,
        /**
         * update
         */
        UPDATE,
        /**
         * insert
         */
        INSERT
    }

    /**
     * @return the specified storage operation
     */
    public StorageOperation getOperation() {
        return this.operation;
    }

    /**
     * This compare method compares the timestamps of the wrapper instances.
     * 
     * @param arg0 -
     *            the wrapper to compare
     * @par
     * @return - 0 if the wrappers timestamp are the same, an integer > 0 if the
     *         given wrapper is after this wrapper
     * 
     */
    public int compareTo(StorageEntryWrapper arg0) {
        return arg0.timestamp == this.timestamp ? 0
                : (arg0.timestamp > this.timestamp ? 1 : -1);
    }

    /**
     * @return - the specified {@link ProvidedServiceConfig}
     */
    public ProvidedService getConfigurator() {
        return this.config;
    }

    /**
     * @return Returns the timestamp.
     */
    public Long getTimestamp() {
        return this.timestamp;
    }

}
