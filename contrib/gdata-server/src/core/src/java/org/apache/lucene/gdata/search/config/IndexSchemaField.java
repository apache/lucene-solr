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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.gdata.search.analysis.ContentStrategy;
import org.apache.lucene.gdata.search.analysis.GdataCategoryStrategy;
import org.apache.lucene.gdata.search.analysis.GdataDateStrategy;
import org.apache.lucene.gdata.search.analysis.HTMLStrategy;
import org.apache.lucene.gdata.search.analysis.KeywordStrategy;
import org.apache.lucene.gdata.search.analysis.MixedContentStrategy;
import org.apache.lucene.gdata.search.analysis.PlainTextStrategy;
import org.apache.lucene.gdata.search.analysis.XHtmlStrategy;
import org.apache.lucene.gdata.utils.ReflectionUtils;

/**
 * Each field in the search index is defined by a instance of
 * {@link IndexSchemaField}. The schema definition will be loaded at startup
 * and the defined values will be set to instances of this class. Each
 * constructed field will be passed to an instance of
 * {@link org.apache.lucene.gdata.search.config.IndexSchema}.
 * <p>
 * IndexSchemaField contains all informations about how the content from
 * incoming entries has to be extracted and how the actual content has to be
 * index into the lucene index.
 * </p>
 * <p>
 * Each field will have a defined
 * {@link org.apache.lucene.gdata.search.analysis.ContentStrategy} which does
 * process the extraction of the field content from an incoming entry.
 * </p>
 * @see org.apache.lucene.gdata.search.analysis.ContentStrategy
 * @see org.apache.lucene.gdata.search.config.IndexSchema
 * 
 * @author Simon Willnauer
 * 
 */
public class IndexSchemaField {
    /**
     * Default value for Field.Store 
     * @see org.apache.lucene.document.Field
     */
    public static final Store DEFAULT_STORE_STRATEGY = Field.Store.NO;
    /**
     * Default value for Field.Index
     * @see org.apache.lucene.document.Field
     */
    public static final Index DEFAULT_INDEX_STRATEGY = Field.Index.TOKENIZED;
    private static final float DEFAULT_BOOST = 1.0f;
    private static final float MINIMAL_BOOST = 0.1f;
    private float boost = DEFAULT_BOOST;

    private String name;

    private ContentType contentType;

    private Index index = DEFAULT_INDEX_STRATEGY;

    private Store store = DEFAULT_STORE_STRATEGY;

    private String path;

    private String typePath;

    private Class<? extends Analyzer> analyzerClass;

    private Class<? extends ContentStrategy> fieldClass;

    /**
     * Constructs a new SchemaField <br>
     * Default values:
     * <ol>
     * <li>boost: <i>1.0</i></li>
     * <li>index: <i>TOKENIZED</i></li>
     * <li>store: <i>NO</i></li>
     * </ol>
     */
    public IndexSchemaField() {
        super();
    }
    boolean checkRequieredValues(){
        /*
         * This class will be inst. by the reg builder.
         * Check all values to be set. otherwise return false.
         * false will cause a runtime exception in IndexSchema
         */
        boolean returnValue = (this.name != null&&this.path!=null&&this.contentType!=null&&this.index!=null&&this.store!=null&&this.boost>=MINIMAL_BOOST);
        if(this.contentType == ContentType.CUSTOM)
            returnValue &=this.fieldClass!=null;
        else if(this.contentType == ContentType.MIXED)
            returnValue &=this.typePath!=null;
        
        return returnValue;
    }
    /**
     * @return Returns the alanyzerClass.
     */
    public Class<? extends Analyzer> getAnalyzerClass() {
        return this.analyzerClass;
    }

    /**
     * @param alanyzerClass
     *            The alanyzerClass to set.
     */
    public void setAnalyzerClass(Class<? extends Analyzer> alanyzerClass) {
        this.analyzerClass = alanyzerClass;
    }

    /**
     * @return Returns the fieldClass.
     */
    public Class<? extends ContentStrategy> getFieldClass() {
        return this.fieldClass;
    }

    /**
     * Sets the class or strategy is used to extract this field Attention: this
     * method set the contentTyp to {@link ContentType#CUSTOM}
     * 
     * @param fieldClass
     *            The fieldClass to set.
     */
    public void setFieldClass(Class<? extends ContentStrategy> fieldClass) {
        if(fieldClass == null)
            throw new IllegalArgumentException("ContentStrategy must not be null");
        if(!ReflectionUtils.extendsType(fieldClass,ContentStrategy.class))
            throw new RuntimeException("The configured ContentStrategy does not extend ContentStrategy, can not use as a custom strategy -- "+fieldClass.getName());
        if(!ReflectionUtils.hasDesiredConstructor(fieldClass,new Class[]{IndexSchemaField.class}))
            throw new RuntimeException("Can not create instance of "+fieldClass.getName());
        this.fieldClass = fieldClass;
        /*
         * set custom - field class is only needed by custom
         */
        this.contentType = ContentType.CUSTOM;
    }

    /**
     * @return Returns the index.
     */
    public Index getIndex() {
        return this.index;
    }

    /**
     * @param index
     *            The index to set.
     */
    public void setIndex(Index index) {
        this.index = index;
    }

    /**
     * @return Returns the name.
     */
    public String getName() {
        return this.name;
    }

    /**
     * @param name
     *            The name to set.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return Returns the path.
     */
    public String getPath() {
        return this.path;
    }

    /**
     * @param path
     *            The path to set.
     */
    public void setPath(String path) {
        this.path = path;
    }

    /**
     * @return Returns the store.
     */
    public Store getStore() {
        return this.store;
    }

    /**
     * @param store
     *            The store to set.
     */
    public void setStore(Store store) {
        this.store = store;
    }

    /**
     * @return Returns the type.
     */
    public ContentType getContentType() {
        return this.contentType;
    }

    /**
     * @param type
     *            The type to set.
     */
    public void setContentType(ContentType type) {
        this.contentType = type;

    }

    /**
     * Sets the content type of this field by the name of the enum type. This
     * method is not case sensitive.
     * 
     * @param type -
     *            type name as string
     */
    public void setType(String type) {
        ContentType[] types = ContentType.class.getEnumConstants();
        for (int i = 0; i < types.length; i++) {
            if (types[i].name().toLowerCase().equals(type)) {
                this.contentType = types[i];
                break;
            }

        }
    }

    /**
     * Defines the {@link ContentStrategy} to use for a
     * <tt>IndexSchemaField</tt> to extract the content from the entry
     * 
     * @author Simon Willnauer
     * 
     */
    public enum ContentType {
       
        /**
         * HTML content strategy {@link HTMLStrategy }
         */
        HTML,
        /**
         * XHTML content strategy {@link XHtmlStrategy }
         */
        XHTML,
        /**
         * Text content strategy {@link PlainTextStrategy }
         */
        TEXT,
        /**
         * GDataDate content strategy {@link GdataDateStrategy }
         */
        GDATADATE,
        /**
         * KEYWORD content strategy {@link KeywordStrategy }
         */
        KEYWORD,
        /**
         * Category content strategy {@link GdataCategoryStrategy }
         */
        CATEGORY,
        /**
         * Custom content strategy (user defined)
         */
        CUSTOM,
        /**
         * Mixed content strategy {@link MixedContentStrategy }
         */
        MIXED

    }

    /**
     * @return Returns the boost.
     */
    public float getBoost() {
        return this.boost;
    }

    /**
     * @param boost
     *            The boost to set.
     */
    public void setBoost(float boost) {
        if (boost <= 0)
            return;
        this.boost = boost;
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(this.getClass()
                .getSimpleName()).append(" ");
        builder.append("field name: ").append(this.name).append(" ");
        builder.append("path: ").append(this.path).append(" ");
        builder.append("content type ").append(this.contentType).append(" ");
        builder.append("field class: ").append(this.fieldClass).append(" ");
        builder.append("analyzer: ").append(this.analyzerClass).append(" ");
        builder.append("boost: ").append(this.boost).append(" ");
        builder.append("INDEX: ").append(this.index).append(" ");
        builder.append("STORE: ").append(this.store);
        return builder.toString();
    }

    /**
     * Sets the Store class by simple name
     * 
     * @param name -
     *            one of yes, no, compress
     */
    public void setStoreByName(String name) {
        if (name.toLowerCase().equals("yes"))
            this.store = Field.Store.YES;
        else if (name.toLowerCase().equals("no"))
            this.store = Field.Store.NO;
        else if (name.toLowerCase().equals("compress"))
            this.store = Field.Store.COMPRESS;
    }

    /**
     * Sets the Index class by simple name
     * 
     * @param name -
     *            un_tokenized, tokenized, no, no_norms
     */
    public void setIndexByName(String name) {
        if (name.toLowerCase().equals("un_tokenized"))
            this.index = Field.Index.UN_TOKENIZED;
        else if (name.toLowerCase().equals("tokenized"))
            this.index = Field.Index.TOKENIZED;
        else if (name.toLowerCase().equals("no_norms"))
            this.index = Field.Index.NO_NORMS;
        else if (name.toLowerCase().equals("no"))
            this.index = Field.Index.NO;
    }

    /**
     * @return Returns the typePath.
     */
    public String getTypePath() {
        return this.typePath;
    }

    /**
     * @param typePath
     *            The typePath to set.
     */
    public void setTypePath(String typePath) {
        this.typePath = typePath;
        /*
         * set Mixed - this property is only needed by mixed type
         */
        setContentType(ContentType.MIXED);
    }

}
