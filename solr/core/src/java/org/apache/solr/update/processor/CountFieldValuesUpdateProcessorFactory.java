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

import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TextField;
import org.apache.solr.schema.StrField;

import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import org.apache.commons.lang.StringUtils;

/**
 * <p>
 * Replaces any list of values for a field matching the specified 
 * conditions with the the count of the number of values for that field.
 * </p>
 * <p>
 * By default, this processor matches no fields.
 * </p>
 * <p>
 * The typical use case for this processor would be in combination with the 
 * {@link CloneFieldUpdateProcessorFactory} so that it's possible to query by 
 * the quantity of values in the source field.
 * <p>
 * For example, in the configuration below, the end result will be that the
 * <code>category_count</code> field can be used to search for documents based 
 * on how many values they contain in the <code>category</code> field.
 * </p>
 *
 * <pre class="prettyprint">
 * &lt;processor class="solr.CloneFieldUpdateProcessorFactory"&gt;
 *   &lt;str name="source"&gt;category&lt;/str&gt;
 *   &lt;str name="dest"&gt;category_count&lt;/str&gt;
 * &lt;/processor&gt;
 * &lt;processor class="solr.CountFieldValuesUpdateProcessorFactory"&gt;
 *   &lt;str name="fieldName"&gt;category_count&lt;/str&gt;
 * &lt;/processor&gt;
 * &lt;processor class="solr.DefaultValueUpdateProcessorFactory"&gt;
 *   &lt;str name="fieldName"&gt;category_count&lt;/str&gt;
 *   &lt;int name="value"&gt;0&lt;/int&gt;
 * &lt;/processor&gt;</pre>
 *
 * <p>
 * <b>NOTE:</b> The use of {@link DefaultValueUpdateProcessorFactory} is 
 * important in this example to ensure that all documents have a value for the 
 * <code>category_count</code> field, because 
 * <code>CountFieldValuesUpdateProcessorFactory</code> only <i>replaces</i> the
 * list of values with the size of that list.  If 
 * <code>DefaultValueUpdateProcessorFactory</code> was not used, then any 
 * document that had no values for the <code>category</code> field, would also 
 * have no value in the <code>category_count</code> field.
 * </p>
 */
public final class CountFieldValuesUpdateProcessorFactory extends FieldMutatingUpdateProcessorFactory {

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req,
                                            SolrQueryResponse rsp,
                                            UpdateRequestProcessor next) {
    return new FieldMutatingUpdateProcessor(getSelector(), next) {
      @Override
      protected SolrInputField mutate(final SolrInputField src) {
        SolrInputField result = new SolrInputField(src.getName());
        result.setValue(src.getValueCount(),
                        src.getBoost());
        return result;
      }
    };
  }
}

