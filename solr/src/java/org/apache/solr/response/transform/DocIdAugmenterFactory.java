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
package org.apache.solr.response.transform;

import java.util.Map;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.request.SolrQueryRequest;

/**
 * @version $Id$
 * @since solr 4.0
 */
public class DocIdAugmenterFactory extends TransformerFactory
{
  @Override
  public DocTransformer create(String field, Map<String,String> args, SolrQueryRequest req) {
    if( !args.isEmpty() ) {
      throw new SolrException( ErrorCode.BAD_REQUEST,
          "DocIdAugmenter does not take any arguments" );
    }
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
    if( docid >= 0 ) {
      doc.setField( name, docid );
    }
  }
}


