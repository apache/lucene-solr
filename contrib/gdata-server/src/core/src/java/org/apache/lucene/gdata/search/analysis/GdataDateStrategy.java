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

import javax.xml.xpath.XPathExpressionException;

import org.apache.lucene.document.Field;
import org.apache.lucene.gdata.data.ServerBaseEntry;
import org.apache.lucene.gdata.search.config.IndexSchemaField;
import org.apache.lucene.gdata.search.index.GdataIndexerException;
import org.w3c.dom.Node;

import com.google.gdata.data.DateTime;

/**
 * This content strategy retrieves a so called GData Date from a RFC 3339
 * timestamp format. The format will be parsed and indexed as a timestamp value.
 * 
 * @author Simon Willnauer
 * 
 */
public class GdataDateStrategy extends ContentStrategy {

    protected GdataDateStrategy(IndexSchemaField fieldConfiguration) {
        super(fieldConfiguration);

    }

    /**
     * @throws NotIndexableException
     * @see org.apache.lucene.gdata.search.analysis.ContentStrategy#processIndexable(org.apache.lucene.gdata.search.analysis.Indexable)
     */
    @Override
    public void processIndexable(
            Indexable<? extends Node, ? extends ServerBaseEntry> indexable)
            throws NotIndexableException {
        String path = this.config.getPath();
        Node node;
        try {
            node = indexable.applyPath(path);
        } catch (XPathExpressionException e1) {
            throw new NotIndexableException("Can not apply path -- " + path
                    + " FieldConfig: " + this.config);
        }
        if (node == null)
            throw new NotIndexableException(
                    "Could not retrieve content for schema field: "
                            + this.config);
        String textContent = node.getTextContent();
        try {
            this.content = getTimeStamp(textContent);
        } catch (NumberFormatException e) {
            throw new NotIndexableException("Can not parse GData date -- "
                    + textContent);
        }

    }

    /**
     * @see org.apache.lucene.gdata.search.analysis.ContentStrategy#createLuceneField()
     */
    @Override
    public Field[] createLuceneField() {
        if(this.fieldName == null)
            throw new GdataIndexerException(
                    "Required field 'name' is null -- " +this.config);
        if(this.content == null)
            throw new GdataIndexerException(
                    "Required field 'content' is null -- " +this.config);
        if(this.content.length()==0)
            return new Field[0];
            Field retValue = new Field(this.fieldName, this.content,
                    Field.Store.YES, Field.Index.UN_TOKENIZED);
            float boost = this.config.getBoost();
            if (boost != 1.0f)
                retValue.setBoost(boost);
            return new Field[] { retValue };
        
    }

    private static String getTimeStamp(String dateString) {
        return Long.toString(DateTime.parseDateTimeChoice(dateString).getValue());
    }

}
