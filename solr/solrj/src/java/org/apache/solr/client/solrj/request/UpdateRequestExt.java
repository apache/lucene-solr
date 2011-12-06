package org.apache.solr.client.solrj.request;

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

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.XML;

// TODO: bake this into UpdateRequest
public class UpdateRequestExt extends AbstractUpdateRequest {
  
  private List<SolrDoc> documents = null;
  private Map<String,Long> deleteById = null;
  private List<String> deleteQuery = null;
  
  private class SolrDoc {
    @Override
    public String toString() {
      return "SolrDoc [document=" + document + ", commitWithin=" + commitWithin
          + ", overwrite=" + overwrite + "]";
    }
    SolrInputDocument document;
    int commitWithin;
    boolean overwrite;
  }
  
  public UpdateRequestExt() {
    super(METHOD.POST, "/update");
  }

  public UpdateRequestExt(String url) {
    super(METHOD.POST, url);
  }
  
  // ---------------------------------------------------------------------------
  // ---------------------------------------------------------------------------
  
  /**
   * clear the pending documents and delete commands
   */
  public void clear() {
    if (documents != null) {
      documents.clear();
    }
    if (deleteById != null) {
      deleteById.clear();
    }
    if (deleteQuery != null) {
      deleteQuery.clear();
    }
  }
  
  // ---------------------------------------------------------------------------
  // ---------------------------------------------------------------------------

  public UpdateRequestExt add(final SolrInputDocument doc) {
    if (documents == null) {
      documents = new ArrayList<SolrDoc>(2);
    }
    SolrDoc solrDoc = new SolrDoc();
    solrDoc.document = doc;
    solrDoc.commitWithin = -1;
    solrDoc.overwrite = true;
    documents.add(solrDoc);
    
    return this;
  }
  
  public UpdateRequestExt add(final SolrInputDocument doc, int commitWithin,
      boolean overwrite) {
    if (documents == null) {
      documents = new ArrayList<SolrDoc>(2);
    }
    SolrDoc solrDoc = new SolrDoc();
    solrDoc.document = doc;
    solrDoc.commitWithin = commitWithin;
    solrDoc.overwrite = overwrite;
    documents.add(solrDoc);
    
    return this;
  }
  
  public UpdateRequestExt deleteById(String id) {
    if (deleteById == null) {
      deleteById = new HashMap<String,Long>();
    }
    deleteById.put(id, null);
    return this;
  }
  
  public UpdateRequestExt deleteById(String id, Long version) {
    if (deleteById == null) {
      deleteById = new HashMap<String,Long>();
    }
    deleteById.put(id, version);
    return this;
  }
  
  public UpdateRequestExt deleteById(List<String> ids) {
    if (deleteById == null) {
      deleteById = new HashMap<String,Long>();
    } else {
      for (String id : ids) {
        deleteById.put(id, null);
      }
    }
    return this;
  }
  
  public UpdateRequestExt deleteByQuery(String q) {
    if (deleteQuery == null) {
      deleteQuery = new ArrayList<String>();
    }
    deleteQuery.add(q);
    return this;
  }
  
  // --------------------------------------------------------------------------
  // --------------------------------------------------------------------------
  
  @Override
  public Collection<ContentStream> getContentStreams() throws IOException {
    return ClientUtils.toContentStreams(getXML(), ClientUtils.TEXT_XML);
  }
  
  public String getXML() throws IOException {
    StringWriter writer = new StringWriter();
    writeXML(writer);
    writer.flush();

    String xml = writer.toString();

    return (xml.length() > 0) ? xml : null;
  }
  
  public void writeXML(Writer writer) throws IOException {
    List<List<SolrDoc>> getDocLists = getDocLists(documents);
    
    for (List<SolrDoc> docs : getDocLists) {
      
      if ((docs != null && docs.size() > 0)) {
        SolrDoc firstDoc = docs.get(0);
        int commitWithin = firstDoc.commitWithin != -1 ? firstDoc.commitWithin : this.commitWithin;
        boolean overwrite = firstDoc.overwrite;
        if (commitWithin > -1 || overwrite != true) {
          writer.write("<add commitWithin=\"" + commitWithin + "\" " + "overwrite=\"" + overwrite + "\">");
        } else {
          writer.write("<add>");
        }
        if (documents != null) {
          for (SolrDoc doc : documents) {
            if (doc != null) {
              ClientUtils.writeXML(doc.document, writer);
            }
          }
        }
        
        writer.write("</add>");
      }
    }
    
    // Add the delete commands
    boolean deleteI = deleteById != null && deleteById.size() > 0;
    boolean deleteQ = deleteQuery != null && deleteQuery.size() > 0;
    if (deleteI || deleteQ) {
      writer.append("<delete>");
      if (deleteI) {
        for (Map.Entry<String,Long> entry : deleteById.entrySet()) {
          writer.append("<id");
          Long version = entry.getValue();
          if (version != null) {
            writer.append(" version=\"" + version + "\"");
          }
          writer.append(">");
          
          XML.escapeCharData(entry.getKey(), writer);
          writer.append("</id>");
        }
      }
      if (deleteQ) {
        for (String q : deleteQuery) {
          writer.append("<query>");
          XML.escapeCharData(q, writer);
          writer.append("</query>");
        }
      }
      writer.append("</delete>");
    }
  }
  
  private List<List<SolrDoc>> getDocLists(List<SolrDoc> documents) {
    List<List<SolrDoc>> docLists = new ArrayList<List<SolrDoc>>();
    if (this.documents == null) {
      return docLists;
    }
    boolean lastOverwrite = true;
    int lastCommitWithin = -1;
    List<SolrDoc> docList = null;
    for (SolrDoc doc : this.documents) {
      if (doc.overwrite != lastOverwrite
          || doc.commitWithin != lastCommitWithin || docLists.size() == 0) {
        docList = new ArrayList<SolrDoc>();
        docLists.add(docList);
      }
      docList.add(doc);
      lastCommitWithin = doc.commitWithin;
      lastOverwrite = doc.overwrite;
    }

    return docLists;
  }

  public Map<String,Long> getDeleteById() {
    return deleteById;
  }
  
  public List<String> getDeleteQuery() {
    return deleteQuery;
  }
  
  @Override
  public String toString() {
    return "UpdateRequestExt [documents=" + documents + ", deleteById="
        + deleteById + ", deleteQuery=" + deleteQuery + "]";
  }
  
}
