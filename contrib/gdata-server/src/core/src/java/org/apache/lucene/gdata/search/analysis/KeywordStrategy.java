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
import org.w3c.dom.Node;

/**
 * @author Simon Willnauer
 *
 */
public class KeywordStrategy extends ContentStrategy {

    /**
     * @param fieldConfig
     */
    public KeywordStrategy(IndexSchemaField fieldConfig) {
        super(Field.Index.UN_TOKENIZED,Field.Store.YES,fieldConfig);
    }

    /**
     * @see org.apache.lucene.gdata.search.analysis.ContentStrategy#processIndexable(org.apache.lucene.gdata.search.analysis.Indexable)
     */
    @Override
    public void processIndexable(Indexable<? extends Node, ? extends ServerBaseEntry> indexable) throws NotIndexableException {
        String path = this.config.getPath();
        try {
            Node node = indexable.applyPath(path);
            if(node == null)
                throw new NotIndexableException("Could not retrieve content for schema field: "+this.config);
            this.content = node.getTextContent();
        } catch (XPathExpressionException e) {
            throw new NotIndexableException("Can not apply Path", e);
        }

    }

   

}
