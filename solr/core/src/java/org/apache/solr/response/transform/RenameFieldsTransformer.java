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

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;

/**
 * Return a field with a name that is different that what is indexed
 *
 *
 * @since solr 4.0
 */
public class RenameFieldsTransformer extends DocTransformer
{
  final NamedList<String> rename;

  public RenameFieldsTransformer( NamedList<String> rename )
  {
    this.rename = rename;
  }

  @Override
  public String getName()
  {
    StringBuilder str = new StringBuilder();
    str.append( "Rename[" );
    for( int i=0; i< rename.size(); i++ ) {
      if( i > 0 ) {
        str.append( "," );
      }
      str.append( rename.getName(i) ).append( ">>" ).append( rename.getVal( i ) );
    }
    str.append( "]" );
    return str.toString();
  }

  @Override
  public void transform(SolrDocument doc, int docid) {
    for( int i=0; i<rename.size(); i++ ) {
      Object v = doc.remove( rename.getName(i) );
      if( v != null ) {
        doc.setField(rename.getVal(i), v);
      }
    }
  }
}
