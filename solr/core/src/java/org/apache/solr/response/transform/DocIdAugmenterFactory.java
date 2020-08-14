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
package org.apache.solr.response.transform;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;

/**
 * Augments the document with a <code>[docid]</code> integer containing it's current
 * (internal) id in the lucene index.  May be <code>-1</code> if this document did not come from the 
 * index (ie: a RealTimeGet from  the transaction log)
 * 
 * @since solr 4.0
 */
public class DocIdAugmenterFactory extends TransformerFactory
{
  @Override
  public DocTransformer create(String field, SolrParams params, SolrQueryRequest req) {
    return new DocIdAugmenter( field );
  }
}

class DocIdAugmenter extends DocTransformer
{
  final String name;

  public DocIdAugmenter( String display )
  {
    this.name = display;
  }

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public void transform(SolrDocument doc, int docid) {
    assert -1 <= docid;
    doc.setField( name, docid );
  }
}


