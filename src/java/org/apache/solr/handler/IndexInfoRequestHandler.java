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

package org.apache.solr.handler;

import org.apache.lucene.index.IndexReader;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrException;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;

import java.util.Collection;
import java.util.Map;
import java.util.HashMap;

/**
 * TODO? delete me? This is now a subset of LukeRequestHandler.  
 * Since it was not released in 1.1 should it be deleted before 1.2?
 */
@Deprecated
public class IndexInfoRequestHandler extends RequestHandlerBase {

  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) {

    try {
      SolrIndexSearcher searcher = req.getSearcher();
      IndexReader reader = searcher.getReader();
      Collection<String> fieldNames = reader.getFieldNames(IndexReader.FieldOption.ALL);
      Map<String,Object> fields = new HashMap<String,Object>();
      IndexSchema schema = req.getSchema();
      for (String fieldName : fieldNames) {
        Map<String,String> fieldInfo = new HashMap<String,String>();

        FieldType fieldType = schema.getFieldTypeNoEx(fieldName);
        if( fieldType != null ) {
          fieldInfo.put("type", fieldType.getTypeName());
        }
        else {
          // This can happen if you change the schema
          fieldInfo.put("type", null ); // "[unknown]"? nothing?
        }

        fields.put(fieldName, fieldInfo);
      }
      rsp.add("fields", fields);

      Map<String,Object> indexInfo = new HashMap<String,Object>();
      indexInfo.put("numDocs", reader.numDocs());
      indexInfo.put("maxDoc", reader.maxDoc());
      indexInfo.put("version", Long.toString(reader.getVersion()));
      // indexInfo.put("age", );  // computed from SolrIndexSearcher.openedAt?

      rsp.add("index", indexInfo);
      
    } catch (SolrException e) {
      rsp.setException(e);
      return;
    } catch (Exception e) {
      SolrException.log(SolrCore.log, e);
      rsp.setException(e);
      return;
    }
  }

  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "The structure Solr request handler";
  }

  @Override
  public String getVersion() {
    return "$Revision: 501512 $";
  }

  @Override
  public String getSourceId() {
    return "$Id: IndexInfoRequestHandler.java 487199 2006-12-14 13:03:40Z bdelacretaz $";
  }

  @Override
  public String getSource() {
    return "$URL: https://svn.apache.org/repos/asf/lucene/solr/trunk/src/java/org/apache/solr/request/IndexInfoRequestHandler.java $";
  }
}


