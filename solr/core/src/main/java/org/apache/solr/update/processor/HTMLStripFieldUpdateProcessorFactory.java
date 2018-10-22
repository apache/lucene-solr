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

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;

import org.apache.commons.io.IOUtils;
import org.apache.lucene.analysis.charfilter.HTMLStripCharFilter;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import static org.apache.solr.update.processor.FieldValueMutatingUpdateProcessor.valueMutator;

/**
 * Strips all HTML Markup in any CharSequence values 
 * found in fields matching the specified conditions.
 * <p>
 * By default this processor matches no fields
 * </p>
 *
 * <p>For example, with the configuration listed below any documents 
 * containing HTML markup in any field declared in the schema using 
 * <code>StrField</code> will have that HTML striped away.
 * </p>
 * <pre class="prettyprint">
 * &lt;processor class="solr.HTMLStripFieldUpdateProcessorFactory"&gt;
 *   &lt;str name="typeClass"&gt;solr.StrField&lt;/str&gt;
 * &lt;/processor&gt;</pre>
 * @since 4.0.0
 */
public final class HTMLStripFieldUpdateProcessorFactory extends FieldMutatingUpdateProcessorFactory {

  @Override
  public FieldMutatingUpdateProcessor.FieldNameSelector 
    getDefaultSelector(final SolrCore core) {

    return FieldMutatingUpdateProcessor.SELECT_NO_FIELDS;

  }
  
  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req,
                                            SolrQueryResponse rsp,
                                            UpdateRequestProcessor next) {
    return valueMutator(getSelector(), next, src -> {
      if (src instanceof CharSequence) {
        CharSequence s = (CharSequence) src;
        StringWriter result = new StringWriter(s.length());
        Reader in = null;
        try {
          in = new HTMLStripCharFilter
              (new StringReader(s.toString()));
          IOUtils.copy(in, result);
          return result.toString();
        } catch (IOException e) {
          // we tried and failed
          return s;
        } finally {
          IOUtils.closeQuietly(in);
        }
      }
      return src;
    });
  }
}

