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

import java.util.ArrayList;
import java.util.List;

import javax.xml.xpath.XPathExpressionException;

import org.apache.lucene.document.Field;
import org.apache.lucene.gdata.data.ServerBaseEntry;
import org.apache.lucene.gdata.search.config.IndexSchemaField;
import org.apache.lucene.gdata.search.index.GdataIndexerException;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

/**
 * This strategy retrieves the category term and and the scheme from a category
 * element. The content is represented by the term which can be configured via
 * the configuration file.
 * <p>
 * The category element has at least one attribute with the name "scheme" which
 * is not mandatory. The term can be the default attribute "term" or the text
 * content of the element, this is configured via the path of the field.
 * </p>
 * <p>
 * <tt>&lt;category scheme="http://www.example.com/type" term="blog.post"/&gt;<tt>
 * </p>
 * TODO extend javadoc for search info
 * @author Simon Willnauer
 *
 */
public class GdataCategoryStrategy extends ContentStrategy {
    protected String categoryScheme;

    protected String categorySchemeField;

    private static final String LABEL = "label";

    private static final String SCHEME = "scheme";

    private static final String TERM = "term";

    /**
     * the string to search a schema if no schema is specified
     */
    public static final String CATEGORY_SCHEMA_NULL_VALUE = "LUCISCHEMANULL";

    /**
     * Schema field suffix
     */
    public static final String CATEGORY_SCHEMA_FIELD_SUFFIX = "-schema";

    protected GdataCategoryStrategy(IndexSchemaField fieldConfiguration) {
        super(fieldConfiguration);
        this.categorySchemeField = new StringBuilder(this.fieldName).append(
                CATEGORY_SCHEMA_FIELD_SUFFIX).toString();

    }

    /**
     * @throws NotIndexableException
     * @see org.apache.lucene.gdata.search.analysis.ContentStrategy#processIndexable(org.apache.lucene.gdata.search.analysis.Indexable)
     */
    @Override
    public void processIndexable(
            Indexable<? extends Node, ? extends ServerBaseEntry> indexable)
            throws NotIndexableException {
        String contentPath = this.config.getPath();
        Node node = null;
        try {
            node = indexable.applyPath(contentPath);
        } catch (XPathExpressionException e) {

            throw new NotIndexableException("Can not apply path");
        }
        if (node == null)
            throw new NotIndexableException(
                    "Could not retrieve content for schema field: "
                            + this.config);

        StringBuilder contentBuilder = new StringBuilder();
        StringBuilder schemeBuilder = new StringBuilder();
        String nodeName = node.getNodeName();
        /*
         * enable more than one category element -- check the node name if
         * category strategy is used with an element not named "category"
         */
        while (node != null && nodeName != null
                && nodeName.equals(node.getNodeName())) {
            NamedNodeMap attributeMap = node.getAttributes();
            if (attributeMap == null)
                throw new NotIndexableException(
                        "category term attribute not present");
            /*
             * the "term" is the internal string used by the software to
             * identify the category, while the "label" is the human-readable
             * string presented to a user in a user interface.
             */
            Node termNode = attributeMap.getNamedItem(TERM);
            if (termNode == null)
                throw new NotIndexableException(
                        "category term attribute not present");
            contentBuilder.append(termNode.getTextContent()).append(" ");

            Node labelNode = attributeMap.getNamedItem(LABEL);
            if (labelNode != null)
                contentBuilder.append(labelNode.getTextContent()).append(" ");

            Node schemeNode = attributeMap.getNamedItem(SCHEME);
            if (schemeNode != null)
                schemeBuilder.append(schemeNode.getTextContent());
            node = node.getNextSibling();
        }

        this.content = contentBuilder.toString();
        this.categoryScheme = schemeBuilder.toString();
    }

    /**
     * @see org.apache.lucene.gdata.search.analysis.ContentStrategy#createLuceneField()
     */
    @Override
    public Field[] createLuceneField() {
        List<Field> retValue = new ArrayList<Field>(2);
        if (this.fieldName == null)
            throw new GdataIndexerException("Required field 'name' is null -- "
                    + this.config);
        if (this.content == null)
            throw new GdataIndexerException(
                    "Required field 'content' is null -- " + this.config);

        Field categoryTerm = new Field(this.fieldName, this.content,
                this.store, this.index);
        float boost = this.config.getBoost();
        if (boost != 1.0f)
            categoryTerm.setBoost(boost);
        retValue.add(categoryTerm);
        /*
         * if schema is not set index null value to enable search for categories
         * without a schema
         */
        if (this.categoryScheme == null || this.categoryScheme.length() == 0) {
            this.categoryScheme = CATEGORY_SCHEMA_NULL_VALUE;
        }
        Field categoryURN = new Field(this.categorySchemeField,
                this.categoryScheme, Field.Store.YES, Field.Index.UN_TOKENIZED);
        retValue.add(categoryURN);
        return retValue.toArray(new Field[0]);

    }

}
