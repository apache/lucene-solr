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
import org.apache.solr.update.processor.FieldMutatingUpdateProcessor.FieldNameSelector;

import java.util.Collections;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;

import static org.apache.solr.update.processor.FieldMutatingUpdateProcessor.SELECT_NO_FIELDS;

/**
 * Keeps only the last value of fields matching the specified 
 * conditions.  Correct behavior assumes that the SolrInputFields being mutated 
 * are either single valued, or use an ordered Collection (ie: not a Set).
 * <p>
 * By default, this processor matches no fields.
 * </p>
 * 
 * <p>
 * For example, in the configuration below, if a field named 
 * <code>primary_author</code> contained multiple values (ie: 
 * <code>"Adam Doe", "Bob Smith", "Carla Jones"</code>) then only the last 
 * value (ie:  <code>"Carla Jones"</code>) will be kept
 * </p>
 *
 * <pre class="prettyprint">
 * &lt;processor class="solr.LastFieldValueUpdateProcessorFactory"&gt;
 *   &lt;str name="fieldName"&gt;primary_author&lt;/str&gt;
 * &lt;/processor&gt;
 * </pre>
 *
 * @see FirstFieldValueUpdateProcessorFactory
 * @since 4.0.0
 */
public final class LastFieldValueUpdateProcessorFactory extends FieldValueSubsetUpdateProcessorFactory {

  @Override
  public Collection<Object> pickSubset(Collection<Object> values) {

    Object result = null;

    if (values instanceof List) {
      // optimize index lookup
      @SuppressWarnings({"rawtypes"})
      List l = (List)values;
      result = l.get(l.size()-1);
    } else if (values instanceof SortedSet) {
      // optimize tail lookup
      result = ((SortedSet)values).last();
    } else {
      // trust the iterator
      for (Object o : values) { result = o; }
    }

    return Collections.singletonList(result);
  }

  @Override
  public FieldNameSelector getDefaultSelector(final SolrCore core) {
    return SELECT_NO_FIELDS;
  }
  
}

