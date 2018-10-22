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

/**
 * Return a field with a name that is different that what is indexed
 *
 *
 * @since solr 4.0
 */
public class RenameFieldTransformer extends DocTransformer
{
  final String from;
  final String to;
  final boolean copy;

  public RenameFieldTransformer( String from, String to, boolean copy )
  {
    this.from = from;
    this.to = to;
    this.copy = copy;
  }

  @Override
  public String getName()
  {
    return "Rename["+from+">>"+to+"]";
  }

  @Override
  public void transform(SolrDocument doc, int docid) {
    Object v = (copy)?doc.get(from) : doc.remove( from );
    if( v != null ) {
      doc.setField(to, v);
    }
  }
}
