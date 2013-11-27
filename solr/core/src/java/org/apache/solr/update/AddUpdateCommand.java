/*
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

package org.apache.solr.update;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;

/**
 *
 */
public class AddUpdateCommand extends UpdateCommand implements Iterable<Document> {
   // optional id in "internal" indexed form... if it is needed and not supplied,
   // it will be obtained from the doc.
   private BytesRef indexedId;

   // Higher level SolrInputDocument, normally used to construct the Lucene Document
   // to index.
   public SolrInputDocument solrDoc;

   public boolean overwrite = true;
   
   public Term updateTerm;

   public int commitWithin = -1;
   
   public AddUpdateCommand(SolrQueryRequest req) {
     super(req);
   }

  @Override
  public String name() {
    return "add";
  }

   /** Reset state to reuse this object with a different document in the same request */
   public void clear() {
     solrDoc = null;
     indexedId = null;
     updateTerm = null;
     version = 0;
   }

   public SolrInputDocument getSolrInputDocument() {
     return solrDoc;
   }

  /** Creates and returns a lucene Document to index.  Any changes made to the returned Document
   * will not be reflected in the SolrInputDocument, or future calls to this method.
   */
   public Document getLuceneDocument() {
     return DocumentBuilder.toDocument(getSolrInputDocument(), req.getSchema());
   }

  /** Returns the indexed ID for this document.  The returned BytesRef is retained across multiple calls, and should not be modified. */
   public BytesRef getIndexedId() {
     if (indexedId == null) {
       IndexSchema schema = req.getSchema();
       SchemaField sf = schema.getUniqueKeyField();
       if (sf != null) {
         if (solrDoc != null) {
           SolrInputField field = solrDoc.getField(sf.getName());

           int count = field==null ? 0 : field.getValueCount();
           if (count == 0) {
             if (overwrite) {
               throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"Document is missing mandatory uniqueKey field: " + sf.getName());
             }
           } else if (count  > 1) {
             throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"Document contains multiple values for uniqueKey field: " + field);
           } else {
             indexedId = new BytesRef();
             sf.getType().readableToIndexed(field.getFirstValue().toString(), indexedId);
           }
         }
       }
     }
     return indexedId;
   }

   public void setIndexedId(BytesRef indexedId) {
     this.indexedId = indexedId;
   }

   public String getPrintableId() {
    if (req != null) {
      IndexSchema schema = req.getSchema();
      SchemaField sf = schema.getUniqueKeyField();
      if (solrDoc != null && sf != null) {
        SolrInputField field = solrDoc.getField(sf.getName());
        if (field != null) {
          return field.getFirstValue().toString();
        }
      }
    }
     return "(null)";
   }

  /**
   * @return String id to hash
   */
  public String getHashableId() {
    String id = null;
    IndexSchema schema = req.getSchema();
    SchemaField sf = schema.getUniqueKeyField();
    if (sf != null) {
      if (solrDoc != null) {
        SolrInputField field = solrDoc.getField(sf.getName());
        
        int count = field == null ? 0 : field.getValueCount();
        if (count == 0) {
          if (overwrite) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                "Document is missing mandatory uniqueKey field: "
                    + sf.getName());
          }
        } else if (count > 1) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "Document contains multiple values for uniqueKey field: " + field);
        } else {
          return field.getFirstValue().toString();
        }
      }
    }
    return id;
  }

  public boolean isBlock() {
    return solrDoc.hasChildDocuments();
  }

  @Override
  public Iterator<Document> iterator() {
    return new Iterator<Document>() {
      Iterator<SolrInputDocument> iter;

      {
        List<SolrInputDocument> all = flatten(solrDoc);

        SchemaField uniq = req.getSchema().getUniqueKeyField();
        String idField = getHashableId();

        for (SolrInputDocument sdoc : all) {
          sdoc.setField("_root_", idField);      // should this be a string or the same type as the ID?
          // TODO: if possible concurrent modification exception (if SolrInputDocument not cloned and is being forwarded to replicas)
          // then we could add this field to the generated lucene document instead.
        }

        iter = all.iterator();
     }

      @Override
      public boolean hasNext() {
        return iter.hasNext();
      }

      @Override
      public Document next() {
        return DocumentBuilder.toDocument(iter.next(), req.getSchema());
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  private List<SolrInputDocument> flatten(SolrInputDocument root) {
    List<SolrInputDocument> unwrappedDocs = new ArrayList<SolrInputDocument>();
    recUnwrapp(unwrappedDocs, root);
    return unwrappedDocs;
  }

  private void recUnwrapp(List<SolrInputDocument> unwrappedDocs, SolrInputDocument currentDoc) {
    List<SolrInputDocument> children = currentDoc.getChildDocuments();
    if (children != null) {
      for (SolrInputDocument child : children) {
        recUnwrapp(unwrappedDocs, child);
      }
    }
    unwrappedDocs.add(currentDoc);
  }


  @Override
  public String toString() {
     StringBuilder sb = new StringBuilder(super.toString());
     sb.append(",id=").append(getPrintableId());
     if (!overwrite) sb.append(",overwrite=").append(overwrite);
     if (commitWithin != -1) sb.append(",commitWithin=").append(commitWithin);
     sb.append('}');
     return sb.toString();
   }


}
