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
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import static org.apache.solr.update.processor.FieldValueMutatingUpdateProcessor.valueMutator;


/**
 * Trims leading and trailing whitespace from any CharSequence values 
 * found in fields matching the specified conditions and returns the 
 * resulting String.
 * <p>
 * By default this processor matches all fields
 * </p>
 *
 * <p>For example, with the configuration listed all String field values 
 * will have leading and trailing spaces removed except for fields whose 
 * named ends with "<code>_literal</code>".
 * </p>
 * <pre class="prettyprint">
 * &lt;processor class="solr.TrimFieldUpdateProcessorFactory"&gt;
 *   &lt;lst name="exclude"&gt;
 *     &lt;str name="fieldRegex"&gt;.*_literal&lt;/str&gt;
 *   &lt;/lst&gt;
 * &lt;/processor&gt;</pre>
 * @since 4.0.0
 */
public final class TrimFieldUpdateProcessorFactory extends FieldMutatingUpdateProcessorFactory {

  @SuppressWarnings("unchecked")
  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {
    // no trim specific init args
    super.init(args);
  }

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req,
                                            SolrQueryResponse rsp,
                                            UpdateRequestProcessor next) {
    return valueMutator(getSelector(), next, src -> {
      if (src instanceof CharSequence) {
        return src.toString().trim();
      }
      return src;
    });
  }
}

