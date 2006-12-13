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
package org.apache.lucene.gdata.search.analysis;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.gdata.data.ServerBaseEntry;
import org.apache.lucene.gdata.search.config.IndexSchemaField;
import org.apache.lucene.gdata.search.config.IndexSchemaField.ContentType;
import org.apache.lucene.gdata.search.index.GdataIndexerException;
import org.apache.lucene.gdata.utils.ReflectionUtils;
import org.w3c.dom.Node;

/**
 * Creating Indexable document requires processing of incoming entities as
 * GData Entries. Entries in the GData protocol might have very different
 * structures and content. They all have on thing in common as they are atom xml
 * format. To retrieve the configured elements of the atom format and process the
 * actual content might differ from element to element.
 * <p>
 * Each predefined ContentStrategy can be used to retrieve certain content from
 * the defined element. Which element to process is defined using a XPath
 * expression in the gdata-config.xml file.
 * </p>
 * <p>
 * <tt>ContentStrategy</tt> implementation should not be accessed directly. To
 * get a <tt>ContentStrategy</tt> for a specific
 * {@link org.apache.lucene.gdata.search.config.IndexSchemaField.ContentType}
 * use the {@link ContentStrategy#getFieldStrategy} factory method. This method
 * expects a IndexSchemaField instance with a set <tt>ContentType</tt>. The
 * return value is a new <tt>ContentStrategy</tt> instance for the defined
 * <tt>ContentType</tt>.
 * </p>
 * 
 * @see org.apache.lucene.gdata.search.config.IndexSchemaField.ContentType
 * @see org.apache.lucene.gdata.search.index.IndexDocumentBuilder
 * 
 * @author Simon Willnauer
 */
public abstract class ContentStrategy {
    protected final Store store;

    protected final Index index;

    protected final IndexSchemaField config;

    protected String content;

    protected String fieldName;

    protected ContentStrategy(IndexSchemaField fieldConfiguration) {
        this(null, null, fieldConfiguration);
    }

    protected ContentStrategy(Index index, Store store,
            IndexSchemaField fieldConfig) {
        if(fieldConfig == null)
            throw new IllegalArgumentException("IndexSchemaField must not be null");
        this.config = fieldConfig;
        this.fieldName = fieldConfig.getName();
        if (index != null) {
            this.index = index;
        } else {
            this.index = fieldConfig.getIndex() == null ? IndexSchemaField.DEFAULT_INDEX_STRATEGY
                    : fieldConfig.getIndex();
        }
        if (store != null) {
            this.store = store;
        } else {
            this.store = fieldConfig.getStore() == null ? IndexSchemaField.DEFAULT_STORE_STRATEGY
                    : fieldConfig.getStore();
        }

    }

    /**
     * @param indexable
     * @throws NotIndexableException
     */
    public abstract void processIndexable(
            Indexable<? extends Node, ? extends ServerBaseEntry> indexable)
            throws NotIndexableException;

    /**
     * This method creates a lucene field from the retrieved content of the
     * entity. Values for Field.Index, Field.Store, the field name and the boost
     * factor are configured in the <tt>IndexSchemaField</tt> passed by the
     * constructor e.g the factory method. This method might be overwritten by
     * subclasses.
     * 
     * @return the Lucene {@link Field}
     */
    public Field[] createLuceneField() {
        /*
         * should I test the content for being empty?!
         * does that make any difference if empty fields are indexed?!
         */
        if(this.fieldName==null|| this.content == null)
            throw new GdataIndexerException("Required field not set fieldName: "+this.fieldName+" content: "+this.content);
        if(this.content.length()==0){
            return new Field[0];
        }
        Field retValue = new Field(this.fieldName, this.content, this.store,
                this.index);
        float boost = this.config.getBoost();
        if (boost != 1.0f)
            retValue.setBoost(boost);
        return new Field[]{retValue};
        
    }

    /**
     * This factory method creates the <tt>ContentStrategy</tt> corresponding
     * to the set <tt>ContentType</tt> value in the <tt>IndexSchemaField</tt>
     * passed to the method as the single parameter.
     * <p>
     * The ContentType must not be null
     * </p>
     * 
     * @param fieldConfig -
     *            the field config to use to identify the corresponding
     *            <tt>ContentStrategy</tt>
     * @return - a new <tt>ContentStrategy</tt> instance
     */
    public static ContentStrategy getFieldStrategy(IndexSchemaField fieldConfig) {
        if (fieldConfig == null)
            throw new IllegalArgumentException(
                    "field configuration must not be null");
        ContentType type = fieldConfig.getContentType();
        if (type == null)
            throw new IllegalArgumentException(
                    "ContentType in IndexSchemaField must not be null");
        fieldConfig.getAnalyzerClass();

        switch (type) {
        case CATEGORY:
            return new GdataCategoryStrategy(fieldConfig);
        case HTML:
            return new HTMLStrategy(fieldConfig);
        case XHTML:
            return new XHtmlStrategy(fieldConfig);
        case GDATADATE:
            return new GdataDateStrategy(fieldConfig);
        case TEXT:
            return new PlainTextStrategy(fieldConfig);
        case KEYWORD:
            return new KeywordStrategy(fieldConfig);
        case CUSTOM:
            /*
             * check if this class can be created with default constructor is checked
             * in IndexSchemaField#setFieldClass and throws RuntimeEx if not. So
             * server can not start up.
             */
            return ReflectionUtils.getDefaultInstance(fieldConfig
                    .getFieldClass());
        case MIXED:
            return new MixedContentStrategy(fieldConfig);
        default:
            throw new RuntimeException("No content strategy found for " + type);
        }

    }

}
