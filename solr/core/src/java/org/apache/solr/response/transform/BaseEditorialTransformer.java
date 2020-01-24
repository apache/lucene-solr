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


import java.util.Set;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.schema.FieldType;

/**
 *
 *
 **/
public abstract class BaseEditorialTransformer extends DocTransformer {

  final String name;
  final String idFieldName;
  final FieldType ft;

  public BaseEditorialTransformer(String name, String idFieldName, FieldType ft) {
    this.name = name;
    this.idFieldName = idFieldName;
    this.ft = ft;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void transform(SolrDocument doc, int docid) {
    //this only gets added if QueryElevationParams.MARK_EXCLUDED is true
    Set<BytesRef> ids = getIdSet();
    if (ids != null && ids.isEmpty() == false) {
      BytesRef key = getKey(doc);
      doc.setField(name, ids.contains(key));
    } else {
      //if we have no ids, that means we weren't marking, but the user still asked for the field to be added, so just mark everything as false
      doc.setField(name, Boolean.FALSE);
    }
  }

  protected abstract Set<BytesRef> getIdSet();

  protected BytesRef getKey(SolrDocument doc) {
    Object obj = doc.get(idFieldName);
    if (obj instanceof IndexableField) {
      IndexableField f = (IndexableField) obj;
      BytesRefBuilder bytesRefBuilder = new BytesRefBuilder();
      Number n = f.numericValue();
      if (n != null) {
        ft.readableToIndexed(n.toString(), bytesRefBuilder);
      } else {
        ft.readableToIndexed(f.stringValue(), bytesRefBuilder);
      }
      return bytesRefBuilder.get();
    } else if (obj instanceof String) { // Allows the idField to be stored=false, docValues=true
      return new BytesRef(((String)obj));
    }
    throw new AssertionError("Expected an IndexableField but got: " + obj.getClass());
  }
}