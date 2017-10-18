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

import org.apache.solr.core.SolrCore;
import org.apache.solr.update.processor.FieldMutatingUpdateProcessor.FieldNameSelector;

import static org.apache.solr.update.processor.FieldMutatingUpdateProcessor.SELECT_NO_FIELDS;

/**
 * Keeps only the first value of fields matching the specified 
 * conditions.  Correct behavior assumes that the SolrInputFields being mutated 
 * are either single valued, or use an ordered Collection (ie: not a Set).
 * <p>
 * By default, this processor matches no fields.
 * </p>
 * 
 * <p>
 * For example, in the configuration below, if a field named 
 * <code>primary_author</code> contained multiple values (ie: 
 * <code>"Adam Doe", "Bob Smith", "Carla Jones"</code>) then only the first 
 * value (ie:  <code>"Adam Doe"</code>) will be kept
 * </p>
 *
 * <pre class="prettyprint">
 * &lt;processor class="solr.FirstFieldValueUpdateProcessorFactory"&gt;
 *   &lt;str name="fieldName"&gt;primary_author&lt;/str&gt;
 * &lt;/processor&gt;
 * </pre>
 *
 * @see LastFieldValueUpdateProcessorFactory
 * @since 4.0.0
 */
public final class FirstFieldValueUpdateProcessorFactory extends FieldValueSubsetUpdateProcessorFactory {

  @Override
  public Collection<Object> pickSubset(Collection<Object> values) {
    // trust the iterator
    return Collections.singletonList(values.iterator().next());
  }

  @Override
  public FieldNameSelector getDefaultSelector(SolrCore core) {
    return SELECT_NO_FIELDS;
  }
  
}

