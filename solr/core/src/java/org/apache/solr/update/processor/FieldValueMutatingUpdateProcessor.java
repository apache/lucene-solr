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

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.function.Function;

import org.apache.solr.common.SolrInputField;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract subclass of FieldMutatingUpdateProcessor for implementing 
 * UpdateProcessors that will mutate all individual values of a selected 
 * field independently
 * 
 * @see FieldMutatingUpdateProcessorFactory
 */
public abstract class FieldValueMutatingUpdateProcessor 
  extends FieldMutatingUpdateProcessor {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  
  public static final Object DELETE_VALUE_SINGLETON = new Object() {
      @Override
      public String toString() { 
        return "!!Singleton Object Triggering Value Deletion!!";
      }
    };

  public FieldValueMutatingUpdateProcessor(FieldNameSelector selector,
                                           UpdateRequestProcessor next) {
    super(selector, next);
  }
  
  /**
   * Mutates individual values of a field as needed, or returns the original 
   * value.
   * 
   * @param src a value from a matched field which should be mutated
   * @return the value to use as a replacement for src, or 
   *         <code>DELETE_VALUE_SINGLETON</code> to indicate that the value 
   *         should be removed completely.
   * @see #DELETE_VALUE_SINGLETON
   */
  protected abstract Object mutateValue(final Object src);
  
  @Override
  protected final SolrInputField mutate(final SolrInputField src) {
    Collection<Object> values = src.getValues();
    if(values == null) return src;//don't mutate
    SolrInputField result = new SolrInputField(src.getName());
    for (final Object srcVal : values) {
      final Object destVal = mutateValue(srcVal);
      if (DELETE_VALUE_SINGLETON == destVal) { 
        /* NOOP */
        if (log.isDebugEnabled()) {
          log.debug("removing value from field '{}': {}",
              src.getName(), srcVal);
        }
      } else {
        if (destVal != srcVal) {
          if (log.isDebugEnabled()) {
            log.debug("replace value from field '{}': {} with {}",
                new Object[]{src.getName(), srcVal, destVal});
          }
        }
        result.addValue(destVal);
      }
    }
    return 0 == result.getValueCount() ? null : result;
  }

  public static FieldValueMutatingUpdateProcessor valueMutator(FieldNameSelector selector,
                                                               UpdateRequestProcessor next,
                                                               Function<Object, Object> fun) {
    return new FieldValueMutatingUpdateProcessor(selector, next) {
      @Override
      protected Object mutateValue(Object src) {
        return fun.apply(src);
      }
    };
  }
}

