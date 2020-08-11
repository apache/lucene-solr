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

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrField;
import org.apache.solr.schema.TextField;

import static org.apache.solr.update.processor.FieldMutatingUpdateProcessor.mutator;

/**
 * Concatenates multiple values for fields matching the specified 
 * conditions using a configurable <code>delimiter</code> which defaults 
 * to "<code>, </code>".
 * <p>
 * By default, this processor concatenates the values for any field name 
 * which according to the schema is <code>multiValued="false"</code> 
 * and uses <code>TextField</code> or <code>StrField</code>
 * </p>
 * 
 * <p>
 * For example, in the configuration below, any "single valued" string and 
 * text field which is found to contain multiple values <i>except</i> for 
 * the <code>primary_author</code> field will be concatenated using the 
 * string "<code>; </code>" as a delimiter.  For the 
 * <code>primary_author</code> field, the multiple values will be left 
 * alone for <code>FirstFieldValueUpdateProcessorFactory</code> to deal with.
 * </p>
 *
 * <pre class="prettyprint">
 * &lt;processor class="solr.ConcatFieldUpdateProcessorFactory"&gt;
 *   &lt;str name="delimiter"&gt;; &lt;/str&gt;
 *   &lt;lst name="exclude"&gt;
 *     &lt;str name="fieldName"&gt;primary_author&lt;/str&gt;
 *   &lt;/lst&gt;
 * &lt;/processor&gt;
 * &lt;processor class="solr.FirstFieldValueUpdateProcessorFactory"&gt;
 *   &lt;str name="fieldName"&gt;primary_author&lt;/str&gt;
 * &lt;/processor&gt;</pre>
 * @since 4.0.0
 */
public final class ConcatFieldUpdateProcessorFactory extends FieldMutatingUpdateProcessorFactory {

  String delimiter = ", ";

  @SuppressWarnings("unchecked")
  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {
    Object d = args.remove("delimiter");
    if (null != d) delimiter = d.toString();

    super.init(args);
  }
  
  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req,
                                            SolrQueryResponse rsp,
                                            UpdateRequestProcessor next) {
    return mutator(getSelector(), next, src -> {
      if (src.getValueCount() <= 1) return src;

      SolrInputField result = new SolrInputField(src.getName());
      result.setValue(StringUtils.join(src.getValues(), delimiter));
      return result;
    });
  }

  @Override
  public FieldMutatingUpdateProcessor.FieldNameSelector 
    getDefaultSelector(final SolrCore core) {

    return fieldName -> {
      final IndexSchema schema = core.getLatestSchema();

      // first check type since it should be fastest
      FieldType type = schema.getFieldTypeNoEx(fieldName);
      if (null == type) return false;

      if (! (TextField.class.isInstance(type)
             || StrField.class.isInstance(type))) {
        return false;
      }

      // only ask for SchemaField if we passed the type check.
      SchemaField sf = schema.getFieldOrNull(fieldName);
      // shouldn't be null since since type wasn't, but just in case
      if (null == sf) return false;

      return ! sf.multiValued();
    };
  }
  
}

