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
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.update.processor.FieldMutatingUpdateProcessor.FieldNameSelector;

import static org.apache.solr.update.processor.FieldMutatingUpdateProcessor.mutator;

/**
 * Ignores &amp; removes fields matching the specified 
 * conditions from any document being added to the index.
 *
 * <p>
 * By default, this processor ignores any field name which does not 
 * exist according to the schema  
 * </p>
 * 
 * <p>
 * For example, in the configuration below, any field name which would cause 
 * an error because it does not exist, or match a dynamicField, in the 
 * schema.xml would be silently removed from any added documents...
 * </p>
 *
 * <pre class="prettyprint">
 * &lt;processor class="solr.IgnoreFieldUpdateProcessorFactory" /&gt;</pre>
 *
 * <p>
 * In this second example, any field name ending in "_raw" found in a 
 * document being added would be removed...
 * </p>
 * <pre class="prettyprint">
 * &lt;processor class="solr.IgnoreFieldUpdateProcessorFactory"&gt;
 *   &lt;str name="fieldRegex"&gt;.*_raw&lt;/str&gt;
 * &lt;/processor&gt;</pre>
 * @since 4.0.0
 */
public final class IgnoreFieldUpdateProcessorFactory extends FieldMutatingUpdateProcessorFactory {

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req,
                                            SolrQueryResponse rsp,
                                            UpdateRequestProcessor next) {
    return mutator(getSelector(), next, src -> null);

  }

  @Override
  public FieldNameSelector getDefaultSelector(final SolrCore core) {
    return fieldName -> {
      final IndexSchema schema = core.getLatestSchema();
      FieldType type = schema.getFieldTypeNoEx(fieldName);
      return (null == type);
    };
  }
  
}

