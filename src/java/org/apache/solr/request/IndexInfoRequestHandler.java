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

package org.apache.solr.request;

import org.apache.lucene.index.IndexReader;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrException;
import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.util.NamedList;
import org.apache.solr.util.SimpleOrderedMap;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.FieldType;
import org.apache.solr.search.SolrIndexSearcher;

import java.net.URL;
import java.util.Collection;
import java.util.Map;
import java.util.HashMap;

public class IndexInfoRequestHandler implements SolrRequestHandler, SolrInfoMBean {
  public void init(NamedList args) {

  }

  public void handleRequest(SolrQueryRequest req, SolrQueryResponse rsp) {

    try {
      SolrIndexSearcher searcher = req.getSearcher();
      IndexReader reader = searcher.getReader();
      Collection<String> fieldNames = reader.getFieldNames(IndexReader.FieldOption.ALL);
      Map fields = new HashMap();
      IndexSchema schema = req.getSchema();
      for (String fieldName : fieldNames) {
        Map fieldInfo = new HashMap();

        FieldType fieldType = schema.getFieldTypeNoEx(fieldName);
        fieldInfo.put("type", fieldType.getTypeName());

        fields.put(fieldName, fieldInfo);
      }
      rsp.add("fields", fields);

      Map indexInfo = new HashMap();
      indexInfo.put("numDocs", reader.numDocs());
      indexInfo.put("maxDoc", reader.maxDoc());
      indexInfo.put("version", reader.getVersion());
      // indexInfo.put("age", );  // computed from SolrIndexSearcher.openedAt?

      rsp.add("index", indexInfo);

      rsp.add("NOTICE","This interface is experimental and may be changing");
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


  public String getName() {
    return IndexInfoRequestHandler.class.getName();
  }

  public String getVersion() {
    return SolrCore.version;
  }

  public String getDescription() {
    return "The structure Solr request handler";
  }

  public Category getCategory() {
    return Category.QUERYHANDLER;
  }

  public String getSourceId() {
    return "$Id: IndexInfoRequestHandler.java 487199 2006-12-14 13:03:40Z bdelacretaz $";
  }

  public String getSource() {
    return "$URL: https://svn.apache.org/repos/asf/lucene/solr/trunk/src/java/org/apache/solr/request/IndexInfoRequestHandler.java $";
  }

  public URL[] getDocs() {
    return null;
  }

  public NamedList getStatistics() {
    return new SimpleOrderedMap();
  }
}

