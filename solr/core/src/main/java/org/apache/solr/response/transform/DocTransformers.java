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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.response.ResultContext;

/**
 * Transform a document before it gets sent out
 *
 *
 */
public class DocTransformers extends DocTransformer
{
  final List<DocTransformer> children = new ArrayList<>();

  @Override
  public String getName()
  {
    StringBuilder str = new StringBuilder();
    str.append( "Transformers[" );
    Iterator<DocTransformer> iter = children.iterator();
    while( iter.hasNext() ) {
      str.append( iter.next().getName() );
      if( iter.hasNext() ) {
        str.append( "," );
      }
    }
    str.append( "]" );
    return str.toString();
  }

  public void addTransformer( DocTransformer a ) {
    children.add( a );
  }

  public int size()
  {
    return children.size();
  }

  public DocTransformer getTransformer( int idx )
  {
    return children.get( idx );
  }

  @Override
  public void setContext( ResultContext context ) {
    for( DocTransformer a : children ) {
      a.setContext( context );
    }
  }

  @Override
  public void transform(SolrDocument doc, int docid, float score) throws IOException {
    for( DocTransformer a : children ) {
      a.transform( doc, docid, score);
    }
  }

  @Override
  public void transform(SolrDocument doc, int docid) throws IOException {
    for( DocTransformer a : children ) {
      a.transform( doc, docid);
    }
  }

  /** Returns true if and only if at least 1 child transformer returns true */
  @Override
  public boolean needsSolrIndexSearcher() {
    for( DocTransformer kid : children ) {
      if (kid.needsSolrIndexSearcher()) {
        return true;
      }
    }
    return false;
  }

}
