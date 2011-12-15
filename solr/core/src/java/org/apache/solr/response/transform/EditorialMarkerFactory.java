package org.apache.solr.response.transform;

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

import java.util.Set;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;

/**
 *
 * @since solr 4.0
 */
public class EditorialMarkerFactory extends TransformerFactory
{
  @Override
  public DocTransformer create(String field, SolrParams params, SolrQueryRequest req) {
    SchemaField uniqueKeyField = req.getSchema().getUniqueKeyField();
    String idfield = uniqueKeyField.getName();
    return new MarkTransformer(field,idfield, uniqueKeyField.getType());
  }
}

class MarkTransformer extends TransformerWithContext
{
  final String name;
  final String idFieldName;
  final FieldType ft;

  public MarkTransformer( String name, String idFieldName, FieldType ft)
  {
    this.name = name;
    this.idFieldName = idFieldName;
    this.ft = ft;
  }

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public void transform(SolrDocument doc, int docid) {
    Set<?> ids = (Set<?>)context.req.getContext().get("BOOSTED");
    if(ids!=null) {
      String key;
      Object field = doc.get(idFieldName);
      if (field instanceof NumericField){
        key = ((Field)field).stringValue();
        key = ft.readableToIndexed(key);
      } else if (field instanceof Field){
        key = ((Field)field).stringValue();
      } else {
        key = field.toString();
      }

      doc.setField(name, ids.contains(key));
    } else {
      //if we have no ids, that means we weren't boosting, but the user still asked for the field to be added, so just mark everything as false
      doc.setField(name, Boolean.FALSE);
    }
  }
}

