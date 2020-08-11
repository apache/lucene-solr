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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.FieldMutatingUpdateProcessor.FieldNameSelector;

import static org.apache.solr.update.processor.FieldMutatingUpdateProcessor.SELECT_NO_FIELDS;
import static org.apache.solr.update.processor.FieldValueMutatingUpdateProcessor.valueMutator;

/**
 * Truncates any CharSequence values found in fields matching the specified 
 * conditions to a maximum character length.
 * <p>
 * By default this processor matches no fields
 * </p>
 *
 * <p>For example, with the configuration listed below any documents 
 * containing a String in any field declared in the schema using 
 * <code>StrField</code> will be truncated to no more then 100 characters
 * </p>
 * <pre class="prettyprint">
 * &lt;processor class="solr.TruncateFieldUpdateProcessorFactory"&gt;
 *   &lt;str name="typeClass"&gt;solr.StrField&lt;/str&gt;
 *   &lt;int name="maxLength"&gt;100&lt;/int&gt;
 * &lt;/processor&gt;</pre>
 * @since 4.0.0
 */
public final class TruncateFieldUpdateProcessorFactory 
  extends FieldMutatingUpdateProcessorFactory {

  private static final String MAX_LENGTH_PARAM = "maxLength";

  private int maxLength = 0;

  @SuppressWarnings("unchecked")
  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {

    Object lengthParam = args.remove(MAX_LENGTH_PARAM);
    if (null == lengthParam) {
      throw new SolrException(ErrorCode.SERVER_ERROR, 
                              "Missing required init parameter: " + 
                              MAX_LENGTH_PARAM);
    }
    if ( ! (lengthParam instanceof Number) ) {
      throw new SolrException(ErrorCode.SERVER_ERROR, 
                              "Init param " + MAX_LENGTH_PARAM + 
                              "must be a number; found: \"" +
                              lengthParam.toString());
    }
    maxLength = ((Number)lengthParam).intValue();
    if (maxLength < 0) {
      throw new SolrException(ErrorCode.SERVER_ERROR, 
                              "Init param " + MAX_LENGTH_PARAM + 
                              "must be >= 0; found: " + maxLength);
    }

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
        CharSequence s = (CharSequence) src;
        if (maxLength < s.length()) {
          return s.subSequence(0, maxLength);
        }
      }
      return src;
    });
  }
}

