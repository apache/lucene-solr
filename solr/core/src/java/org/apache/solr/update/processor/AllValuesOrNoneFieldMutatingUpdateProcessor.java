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

import org.apache.solr.common.SolrInputField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

/**
 * Abstract subclass of FieldMutatingUpdateProcessor for implementing 
 * UpdateProcessors that will mutate all individual values of a selected 
 * field independently.  If not all individual values are acceptable
 * - i.e., mutateValue(srcVal) returns {@link #SKIP_FIELD_VALUE_LIST_SINGLETON}
 * for at least one value - then none of the values are mutated:
 * mutate(srcField) will return srcField.
 *
 * @see FieldMutatingUpdateProcessorFactory
 * @see FieldValueMutatingUpdateProcessor
 */
public abstract class AllValuesOrNoneFieldMutatingUpdateProcessor extends FieldMutatingUpdateProcessor {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final Object DELETE_VALUE_SINGLETON = new Object() {
    @Override
    public String toString() {
      return "!!Singleton Object Triggering Value Deletion!!";
    }
  };

  public static final Object SKIP_FIELD_VALUE_LIST_SINGLETON= new Object() {
    @Override
    public String toString() {
      return "!!Singleton Object Triggering Skipping Field Mutation!!";
    }
  };


  public AllValuesOrNoneFieldMutatingUpdateProcessor(FieldNameSelector selector, UpdateRequestProcessor next) {
    super(selector, next);
  }

  /**
   * Mutates individual values of a field as needed, or returns the original 
   * value.
   *
   * @param srcVal a value from a matched field which should be mutated
   * @return the value to use as a replacement for src, or 
   *         <code>DELETE_VALUE_SINGLETON</code> to indicate that the value 
   *         should be removed completely, or
   *         <code>SKIP_FIELD_VALUE_LIST_SINGLETON</code> to indicate that
   *         a field value is not consistent with 
   * @see #DELETE_VALUE_SINGLETON
   * @see #SKIP_FIELD_VALUE_LIST_SINGLETON
   */
  protected abstract Object mutateValue(final Object srcVal);

  protected final SolrInputField mutate(final SolrInputField srcField) {
    Collection<Object> vals = srcField.getValues();
    if(vals== null || vals.isEmpty()) return srcField;
    List<String> messages = null;
    SolrInputField result = new SolrInputField(srcField.getName());
    for (final Object srcVal : vals) {
      final Object destVal = mutateValue(srcVal);
      if (SKIP_FIELD_VALUE_LIST_SINGLETON == destVal) {
        if (log.isDebugEnabled()) {
          log.debug("field '{}' {} value '{}' is not mutable, so no values will be mutated",
              new Object[]{srcField.getName(), srcVal.getClass().getSimpleName(), srcVal});
        }
        return srcField;
      }
      if (DELETE_VALUE_SINGLETON == destVal) {
        if (log.isDebugEnabled()) {
          if (null == messages) {
            messages = new ArrayList<>();
          }
          messages.add(String.format(Locale.ROOT, "removing value from field '%s': %s '%s'", 
                                     srcField.getName(), srcVal.getClass().getSimpleName(), srcVal));
        }
      } else {
        if (log.isDebugEnabled()) {
          if (null == messages) {
            messages = new ArrayList<>();
          }
          messages.add(String.format(Locale.ROOT, "replace value from field '%s': %s '%s' with %s '%s'", 
                                     srcField.getName(), srcVal.getClass().getSimpleName(), srcVal, 
                                     destVal.getClass().getSimpleName(), destVal));
        }
        result.addValue(destVal);
      }
    }
    
    if (null != messages && log.isDebugEnabled()) {
      for (String message : messages) {
        log.debug(message);
      }
    }
    return 0 == result.getValueCount() ? null : result;
  }
}
