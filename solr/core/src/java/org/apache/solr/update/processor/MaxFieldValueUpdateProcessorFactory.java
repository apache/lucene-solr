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

import java.util.Collection;
import java.util.Collections;

import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrCore;
import org.apache.solr.update.processor.FieldMutatingUpdateProcessor.FieldNameSelector;

import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;
import static org.apache.solr.update.processor.FieldMutatingUpdateProcessor.SELECT_NO_FIELDS;

/**
 * An update processor that keeps only the the maximum value from any selected 
 * fields where multiple values are found.  Correct behavior requires tha all 
 * of the values in the SolrInputFields being mutated are mutually comparable; 
 * If this is not the case, then a SolrException will br thrown. 
 * <p>
 * By default, this processor matches no fields.
 * </p>
 *
 * <p>
 * In the example configuration below, if a document contains multiple integer 
 * values (ie: <code>64, 128, 1024</code>) in the field 
 * <code>largestFileSize</code> then only the biggest value 
 * (ie: <code>1024</code>) will be kept in that field.
 * <br>
 *
 * <pre class="prettyprint">
 *  &lt;processor class="solr.MaxFieldValueUpdateProcessorFactory"&gt;
 *    &lt;str name="fieldName"&gt;largestFileSize&lt;/str&gt;
 *  &lt;/processor&gt;
 * </pre>
 *
 * @see MinFieldValueUpdateProcessorFactory
 * @see Collections#max
 * @since 4.0.0
 */
public final class MaxFieldValueUpdateProcessorFactory extends FieldValueSubsetUpdateProcessorFactory {

  @Override
  @SuppressWarnings({"unchecked"})
  public Collection<Object> pickSubset(@SuppressWarnings({"rawtypes"})Collection values) {
    @SuppressWarnings({"rawtypes"})
    Collection result = values;
    try {
      // NOTE: the extra cast to Object is needed to prevent compile
      // errors on Eclipse Compiler (ecj) used for javadoc lint
      result = Collections.singletonList((Object) Collections.max(values));
    } catch (ClassCastException e) {
      throw new SolrException
        (BAD_REQUEST, 
         "Field values are not mutually comparable: " + e.getMessage(), e);
    }
    return result;
  }

  @Override
  public FieldNameSelector getDefaultSelector(SolrCore core) {
    return SELECT_NO_FIELDS;
  }
  
}

