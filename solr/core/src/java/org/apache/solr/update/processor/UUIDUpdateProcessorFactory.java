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
package org.apache.solr.update.processor;

import java.util.UUID;
import java.util.Locale;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.SchemaField;


/**
 * <p>
 * An update processor that adds a newly generated <code>UUID</code> value
 * to any document being added that does not already have a value in the
 * specified field.
 * </p>
 *
 * <p>
 * In the example configuration below, if a document does not contain a value
 * in the <code>id</code> field, a new <code>UUID</code> will be generated
 * and added as the value of that field.
 * <br>
 *
 * <pre class="prettyprint">
 * &lt;processor class="solr.UUIDUpdateProcessorFactory"&gt;
 *   &lt;str name="fieldName"&gt;id&lt;/str&gt;
 * &lt;/processor&gt;
 * </pre>
 *
 *
 * <p>
 * You can also invoke the processor with request handler param(s)
 * as <code>uuid.fieldname</code> with <code>processor=uuid</code>
 *
 * curl -X POST -H Content-Type: application/json
 * http://localhost:8983/solr/test/update/json/docs?processor=uuid;ampersand;uuid.fieldName=id;ampersand;commit=true
 * --data-binary {"id":"1","title": "titleA"}
 *
 * NOTE: The param(s) provided in request handler will override / supersede processor's config.
 *
 * If field name is omitted in processor configuration and not provided in request handler param(s),
 * then  @{link org.apache.solr.schema.IndexSchema#getUniqueKeyField()}
 * is used as field and a new <code>UUID</code> will be generated
 * and added as the value of that field. The field type of the uniqueKeyField
 * must be anything which accepts a string or UUID value.
 *
 *
 *
 * @see UUID
 * @since 4.0.0
 */
public class UUIDUpdateProcessorFactory extends UpdateRequestProcessorFactory {

  private static final String PREFIX_PARAM = "uuid.";
  public static final String NAME = "uuid";
  private static final String FIELD_PARAM = "fieldName";


  protected String fieldName = null;

  @SuppressWarnings("unchecked")
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {

    Object obj = args.remove(FIELD_PARAM);
    if (null != obj) {
      fieldName = obj.toString();
    }
  }

  public UpdateRequestProcessor getInstance(SolrQueryRequest req,
                                            SolrQueryResponse rsp,
                                            UpdateRequestProcessor next ) {
    String fieldName = this.fieldName;

    String fname = req.getParams().get(PREFIX_PARAM+FIELD_PARAM);
    if (!StringUtils.isEmpty(fname)) {
      fieldName = fname;
    }

    if (StringUtils.isEmpty(fieldName)) {
      SchemaField schemaField = req.getSchema().getUniqueKeyField();
      fieldName = schemaField.getName();
    }

    return new AbstractDefaultValueUpdateProcessorFactory.DefaultValueUpdateProcessor(fieldName, next) {
      @Override
      public Object getDefaultValue() {
        return UUID.randomUUID().toString().toLowerCase(Locale.ROOT);
      }
    };
  }
}



