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
import org.apache.lucene.gdata.data.ServerBaseFeed;
import org.apache.lucene.gdata.server.registry.ProvidedService;

import com.google.gdata.data.BaseFeed;
import com.google.gdata.util.common.xml.XmlWriter;

/**
 * This immutable class wrapps <tt>GDataAccount</tt> instances for an internal Storage representation of 
 * an account. This class also acts as a Documentfactory for lucene documents to 
 * be stored inside the index. 
 * @author Simon Willnauer
 *
 */
public class StorageFeedWrapper implements StorageWrapper {
    
    private static final String INTERNAL_ENCODING = "UTF-8";
    /**
     * the account who owns the feed 
     */
    public static final String FIELD_ACCOUNTREFERENCE = "accountReference";
    /**
     * the id of the feed
     */
    public static final String FIELD_FEED_ID = "feedId";
    /**
     * The xml feed representation
     */
    public static final String FIELD_CONTENT = "content";
    
    /**
     * The creation timestamp
     */
    public static final String FIELD_TIMESTAMP = "timestamp";

    /**
     * The Service this feed belongs to. 
     */
    public static final String FIELD_SERVICE_ID = "serviceId";
    private final ServerBaseFeed feed;
    private final String accountName;
    private final ProvidedService config;
    private final String content;
    
    
    /**
     * @param feed 
     * @param accountname 
     * @throws IOException 
     * 
     */
    public StorageFeedWrapper(final ServerBaseFeed feed, final String accountname) throws IOException {
        this.feed = feed;
        this.accountName = accountname;
        this.config = feed.getServiceConfig();
        this.content = buildContent();
       
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageWrapper#getLuceneDocument()
     */
    public Document getLuceneDocument() {
        Document doc = new Document();
        doc.add(new Field(FIELD_ACCOUNTREFERENCE,this.accountName,Field.Store.YES,Field.Index.UN_TOKENIZED));
        doc.add(new Field(FIELD_FEED_ID,this.feed.getId(),Field.Store.YES,Field.Index.UN_TOKENIZED));
        doc.add(new Field(FIELD_CONTENT,this.content,Field.Store.COMPRESS,Field.Index.NO));
        doc.add(new Field(FIELD_SERVICE_ID,this.feed.getServiceType(),Field.Store.YES,Field.Index.NO));
        return doc;
    }

    private String buildContent() throws IOException { 
        StringWriter writer = new StringWriter(); 
        XmlWriter xmlWriter = new XmlWriter(writer, INTERNAL_ENCODING); 
        this.feed.generateAtom(xmlWriter,this.config.getExtensionProfile()); 
        return writer.toString(); 
    }
    /**
     * @return - the wrapped feed
     */
    public BaseFeed getFeed(){
        return this.feed.getFeed();
    }
}
