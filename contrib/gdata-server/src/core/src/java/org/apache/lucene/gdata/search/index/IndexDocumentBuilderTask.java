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

import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.gdata.data.ServerBaseEntry;
import org.apache.lucene.gdata.search.analysis.ContentStrategy;
import org.apache.lucene.gdata.search.analysis.Indexable;
import org.apache.lucene.gdata.search.analysis.NotIndexableException;
import org.apache.lucene.gdata.search.config.IndexSchema;
import org.apache.lucene.gdata.search.config.IndexSchemaField;

/**
 * This callable does all of the entiti processing concurrently while added to
 * the {@link org.apache.lucene.gdata.search.index.GDataIndexer} task queue;
 * 
 * @see org.apache.lucene.gdata.search.analysis.Indexable
 * @see org.apache.lucene.gdata.search.analysis.ContentStrategy
 * @author Simon Willnauer
 * 
 */
class IndexDocumentBuilderTask<T extends IndexDocument> implements IndexDocumentBuilder<T> {
    private static final Log LOG = LogFactory
            .getLog(IndexDocumentBuilderTask.class);

    private final ServerBaseEntry entry;

    private final IndexSchema schema;

    private final IndexAction action;

    private final boolean commitAfter;
    private final boolean optimizeAfter;
    protected IndexDocumentBuilderTask(final ServerBaseEntry entry,
            final IndexSchema schema, IndexAction action, boolean commitAfter, boolean optimizeAfter) {
        /*
         * omit check for null parameter this happens in the controller.
         */
        this.schema = schema;
        this.entry = entry;
        this.action = action;
        this.commitAfter = commitAfter;
        this.optimizeAfter = optimizeAfter;
    }

    /**
     * @see java.util.concurrent.Callable#call()
     */
    @SuppressWarnings("unchecked")
    public T call() throws GdataIndexerException {
        
        Collection<IndexSchemaField> fields = this.schema.getFields();
        GDataIndexDocument document = new GDataIndexDocument(this.action,
                this.entry.getId(),this.entry.getFeedId(), this.commitAfter,this.optimizeAfter);
        if(this.action != IndexAction.DELETE){
        int addedFields = 0;
        for (IndexSchemaField field : fields) {
            /*
             * get the strategy to process the field
             */
            ContentStrategy strategy = ContentStrategy.getFieldStrategy(field);
            if (LOG.isInfoEnabled())
                LOG.info("Process indexable for " + field);
            try {
                /*
                 * get the indexable via the factory method to enable new /
                 * different implementation of the interface (this could be a
                 * faster dom impl e.g. dom4j)
                 */
                strategy.processIndexable(Indexable.getIndexable(this.entry));
                addedFields++;
            } catch (NotIndexableException e) {
                LOG.warn("Can not create field for " + field+" field will be skipped -- reason: ", e);
                continue;
            }
         
            document.addField(strategy);

        }
        if(addedFields == 0)
            throw new GdataIndexerException("No field added to document for Schema: "+this.schema); 
        }
        return (T)document;
    }

    
}
