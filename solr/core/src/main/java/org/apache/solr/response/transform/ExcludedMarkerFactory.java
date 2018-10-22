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

import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.component.QueryElevationComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;

/**
 *
 * @since solr 4.0
 */
public class ExcludedMarkerFactory extends TransformerFactory
{

  @Override
  public DocTransformer create(String field, SolrParams params, SolrQueryRequest req) {
    SchemaField uniqueKeyField = req.getSchema().getUniqueKeyField();
    String idfield = uniqueKeyField.getName();
    return new ExcludedTransformer(field,idfield, uniqueKeyField.getType());
  }
}

class ExcludedTransformer extends BaseEditorialTransformer {

  public ExcludedTransformer( String name, String idFieldName, FieldType ft)
  {
    super(name, idFieldName, ft);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Set<BytesRef> getIdSet() {
    return (Set<BytesRef>)context.getRequest().getContext().get(QueryElevationComponent.EXCLUDED);
  }

}

