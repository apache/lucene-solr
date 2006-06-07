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
 
import com.google.gdata.data.BaseEntry; 
import com.google.gdata.data.ExtensionProfile; 
import com.google.gdata.util.common.xml.XmlWriter; 
 
/** 
 * This immutable class wrapps Entries for an internal Storage representation of 
 * an entry. This class also acts as a Documentfactory for lucene documents to 
 * be stored inside the index. 
 *  
 * @author Simon Willnauer 
 *  
 */ 
public class StorageEntryWrapper implements Comparable<StorageEntryWrapper> { 
    private static final String INTERNAL_ENCODING = "UTF-8"; 
 
    /** 
     * lucene field name Entry id 
     */ 
    public final static String FIELD_ENTRY_ID = "entryId"; 
 
    /** 
     * lucene field name feed id 
     */ 
    public final static String FIELD_FEED_ID = "feedId"; 
 
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
 
    private final String content; 
 
    private final transient BaseEntry entry; 
 
    private final Long timestamp; 
 
    private transient Document document; 
 
    private StorageOperation operation; 
 
    private final ExtensionProfile profile; 
 
    /** 
     * Creates a new StorageEntryWrapper. 
     *  
     * @param entry - 
     *            the entry to wrap 
     * @param feedId - 
     *            the feed id 
     * @param operation - 
     *            the StorageOperation 
     * @param profile - 
     *            the ExtensionProfil for the given entry 
     * @throws IOException - 
     *             if the entry content can not be generated 
     */ 
    protected StorageEntryWrapper(final BaseEntry entry, final String feedId, 
            StorageOperation operation, final ExtensionProfile profile) 
            throws IOException { 
        this.entry = entry; 
        this.operation = operation; 
        this.entryId = entry.getId(); 
        this.feedId = feedId; 
        this.profile = profile; 
        this.content = buildContent(); 
        this.timestamp = new Long(System.currentTimeMillis()); 
 
    } 
 
    private String buildContent() throws IOException { 
        StringWriter writer = new StringWriter(); 
        XmlWriter xmlWriter = new XmlWriter(writer, INTERNAL_ENCODING); 
        this.entry.generateAtom(xmlWriter, this.profile); 
        return writer.toString(); 
 
    } 
 
    /** 
     * @return - the lucene document representing the entry 
     */ 
    public Document getLuceneDocument() { 
        if (this.document != null) 
            return this.document; 
        this.document = new Document(); 
        this.document.add(new Field("entryId", this.entryId, Field.Store.YES, 
                Field.Index.UN_TOKENIZED)); 
        this.document.add(new Field("feedId", this.feedId, Field.Store.YES, 
                Field.Index.UN_TOKENIZED)); 
        this.document.add(new Field("content", this.content, 
                Field.Store.COMPRESS, Field.Index.UN_TOKENIZED)); 
        this.document.add(new Field("timestamp", this.timestamp.toString(), 
                Field.Store.YES, Field.Index.UN_TOKENIZED)); 
 
        return this.document; 
 
    } 
 
    /** 
     * @return - the wrapped entry 
     */ 
    public BaseEntry getEntry() { 
        return this.entry; 
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
     * @see java.lang.Comparable#compareTo(T) 
     */ 
    public int compareTo(StorageEntryWrapper arg0) { 
        return arg0.timestamp == this.timestamp ? 0 
                : (arg0.timestamp > this.timestamp ? 1 : -1); 
    } 
 
} 
