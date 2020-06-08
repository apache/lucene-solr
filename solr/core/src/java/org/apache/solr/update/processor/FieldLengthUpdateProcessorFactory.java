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

import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.FieldMutatingUpdateProcessor.FieldNameSelector;

import static org.apache.solr.update.processor.FieldMutatingUpdateProcessor.SELECT_NO_FIELDS;
import static org.apache.solr.update.processor.FieldValueMutatingUpdateProcessor.valueMutator;


/**
 * Replaces any CharSequence values found in fields matching the specified 
 * conditions with the lengths of those CharSequences (as an Integer).
 * <p>
 * By default, this processor matches no fields.
 * </p>
 * <p>For example, with the configuration listed below any documents 
 * containing  String values (such as "<code>abcdef</code>" or 
 * "<code>xyz</code>") in a field declared in the schema using 
 * <code>IntPointField</code> or <code>LongPointField</code> 
 * would have those Strings replaced with the length of those fields as an 
 * Integer 
 * (ie: <code>6</code> and <code>3</code> respectively)
 * </p>
 * <pre class="prettyprint">
 * &lt;processor class="solr.FieldLengthUpdateProcessorFactory"&gt;
 *   &lt;arr name="typeClass"&gt;
 *     &lt;str&gt;solr.IntPointField&lt;/str&gt;
 *     &lt;str&gt;solr.LongPointField&lt;/str&gt;
 *   &lt;/arr&gt;
 * &lt;/processor&gt;</pre>
 * @since 4.0.0
 */
public final class FieldLengthUpdateProcessorFactory extends FieldMutatingUpdateProcessorFactory {

  @SuppressWarnings("unchecked")
  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {
    // no length specific init args
    super.init(args);
  }

  @Override
  public FieldNameSelector getDefaultSelector(final SolrCore core) {
    return SELECT_NO_FIELDS;
  }

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req,
                                            SolrQueryResponse rsp,
                                            UpdateRequestProcessor next) {
    return valueMutator(getSelector(), next, src -> {
      if (src instanceof CharSequence) {
        return ((CharSequence) src).length();
      }
      return src;
    });

  }
}

