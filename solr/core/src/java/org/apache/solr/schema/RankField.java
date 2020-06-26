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
package org.apache.solr.schema;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.document.FeatureField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.solr.common.SolrException;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.uninverting.UninvertingReader.Type;

public class RankField extends FieldType {

  @Override
  public Type getUninversionType(SchemaField sf) {
    throw null;
  }

  @Override
  public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
  }
  
  @Override
  protected void init(IndexSchema schema, Map<String,String> args) {
    super.init(schema, args);
    properties &= ~(UNINVERTIBLE | STORED | DOC_VALUES);
    
  }

  @Override
  protected IndexableField createField(String name, String val, IndexableFieldType type) {
    if (val == null || val.isEmpty()) {
      return null;
    }
    String[] parts = val.split(":");
    if (parts.length != 2) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Error while creating field '" + name + "' from value '" + val + "'. Expecting format to be 'feature:value'");
    }
    float featureValue;
    try {
      featureValue = Float.parseFloat(parts[1]);
    } catch (NumberFormatException nfe) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Error while creating field '" + name + "' from value '" + val + "'. Expecting format to be 'feature:value'", nfe);
    }
    return new FeatureField(name, parts[0], featureValue);
  }

  @Override
  public Query getFieldQuery(QParser parser, SchemaField field, String externalVal) {
    return super.getFieldQuery(parser, field, externalVal);
  }

  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, 
        "can not sort on a rank field: " + field.getName());
  }

}
