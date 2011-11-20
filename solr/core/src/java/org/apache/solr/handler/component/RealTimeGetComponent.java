package org.apache.solr.handler.component;

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

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.response.transform.DocTransformer;
import org.apache.solr.response.transform.TransformContext;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.ReturnFields;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.DocumentBuilder;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.util.RefCounted;

import javax.xml.transform.Transformer;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * TODO!
 * 
 *
 * @since solr 1.3
 */
public class RealTimeGetComponent extends SearchComponent
{
  public static final String COMPONENT_NAME = "get";

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    // Set field flags
    ReturnFields returnFields = new ReturnFields( rb.req );
    rb.rsp.setReturnFields( returnFields );
  }


  @Override
  public void process(ResponseBuilder rb) throws IOException
  {
    SolrQueryRequest req = rb.req;
    SolrQueryResponse rsp = rb.rsp;
    SolrParams params = req.getParams();

    if (!params.getBool(COMPONENT_NAME, true)) {
      return;
    }

    String id[] = params.getParams("id");
    String ids[] = params.getParams("ids");

    if (id == null && ids == null) {
      return;
    }

    String[] allIds = id==null ? new String[0] : id;

    if (ids != null) {
      List<String> lst = new ArrayList<String>();
      for (String s : allIds) {
        lst.add(s);
      }
      for (String idList : ids) {
        lst.addAll( StrUtils.splitSmart(idList, ",", true) );
      }
      allIds = lst.toArray(new String[lst.size()]);
    }

    SchemaField idField = req.getSchema().getUniqueKeyField();
    FieldType fieldType = idField.getType();

    SolrDocumentList docList = new SolrDocumentList();
    UpdateLog ulog = req.getCore().getUpdateHandler().getUpdateLog();

    RefCounted<SolrIndexSearcher> searcherHolder = null;

    DocTransformer transformer = rsp.getReturnFields().getTransformer();
    if (transformer != null) {
      TransformContext context = new TransformContext();
      context.req = req;
      transformer.setContext(context);
    }
   try {
     SolrIndexSearcher searcher = null;

     BytesRef idBytes = new BytesRef();
     for (String idStr : allIds) {
       fieldType.readableToIndexed(idStr, idBytes);
       if (ulog != null) {
         Object o = ulog.lookup(idBytes);
         if (o != null) {
           // should currently be a List<Oper,Ver,Doc/Id>
           List entry = (List)o;
           assert entry.size() >= 3;
           int oper = (Integer)entry.get(0);
           switch (oper) {
             case UpdateLog.ADD:
               SolrDocument doc = toSolrDoc((SolrInputDocument)entry.get(entry.size()-1), req.getSchema());
               if(transformer!=null) {
                 transformer.transform(doc, -1); // unknown docID
               }
              docList.add(doc);
              break;
             case UpdateLog.DELETE:
              break;
             default:
               throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,  "Unknown Operation! " + oper);
           }
           continue;
         }
       }

       // didn't find it in the update log, so it should be in the newest searcher opened
       if (searcher == null) {
         searcherHolder =  req.getCore().getNewestSearcher(false);
         searcher = searcherHolder.get();
       }

       // SolrCore.verbose("RealTimeGet using searcher ", searcher);

       int docid = searcher.getFirstMatch(new Term(idField.getName(), idBytes));
       if (docid < 0) continue;
       Document luceneDocument = searcher.doc(docid);
       SolrDocument doc = toSolrDoc(luceneDocument,  req.getSchema());
       if( transformer != null ) {
         transformer.transform(doc, docid);
       }
       docList.add(doc);
     }

   } finally {
     if (searcherHolder != null) {
       searcherHolder.decref();
     }
   }


   // if the client specified a single id=foo, then use "doc":{
   // otherwise use a standard doclist

   if (ids ==  null && allIds.length <= 1) {
     // if the doc was not found, then use a value of null.
     rsp.add("doc", docList.size() > 0 ? docList.get(0) : null);
   } else {
     docList.setNumFound(docList.size());
     rsp.add("response", docList);
   }

  }

  private static SolrDocument toSolrDoc(Document doc, IndexSchema schema) {
    SolrDocument out = new SolrDocument();
    for( IndexableField f : doc.getFields() ) {
      // Make sure multivalued fields are represented as lists
      Object existing = out.get(f.name());
      if (existing == null) {
        SchemaField sf = schema.getFieldOrNull(f.name());
        if (sf != null && sf.multiValued()) {
          List<Object> vals = new ArrayList<Object>();
          vals.add( f );
          out.setField( f.name(), vals );
        }
        else{
          out.setField( f.name(), f );
        }
      }
      else {
        out.addField( f.name(), f );
      }
    }
    return out;
  }

  private static SolrDocument toSolrDoc(SolrInputDocument sdoc, IndexSchema schema) {
    // TODO: do something more performant than this double conversion
    Document doc = DocumentBuilder.toDocument(sdoc, schema);
    List<IndexableField> fields = doc.getFields();

    // copy the stored fields only
    Document out = new Document();
    for (IndexableField f : doc.getFields()) {
      if (f.fieldType().stored()) {
        out.add(f);
      }
    }

    return toSolrDoc(out, schema);
  }



  ////////////////////////////////////////////
  ///  SolrInfoMBean
  ////////////////////////////////////////////

  @Override
  public String getDescription() {
    return "query";
  }

  @Override
  public String getVersion() {
    return "$Revision$";
  }

  @Override
  public String getSourceId() {
    return "$Id$";
  }

  @Override
  public String getSource() {
    return "$URL$";
  }

  @Override
  public URL[] getDocs() {
    return null;
  }
}
