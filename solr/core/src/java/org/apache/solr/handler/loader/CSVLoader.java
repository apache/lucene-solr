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
package org.apache.solr.handler.loader;

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import java.io.*;

public class CSVLoader extends ContentStreamLoader {
  @Override
  public void load(SolrQueryRequest req, SolrQueryResponse rsp,
      ContentStream stream, UpdateRequestProcessor processor) throws Exception {
    new SingleThreadedCSVLoader(req,processor).load(req, rsp, stream, processor);
  }
}

class SingleThreadedCSVLoader extends CSVLoaderBase {
  SingleThreadedCSVLoader(SolrQueryRequest req, UpdateRequestProcessor processor) {
    super(req, processor);
  }

  @Override
  public void addDoc(int line, String[] vals) throws IOException {
    templateAdd.clear();
    SolrInputDocument doc = new SolrInputDocument();
    doAdd(line, vals, doc, templateAdd);
  }
}
