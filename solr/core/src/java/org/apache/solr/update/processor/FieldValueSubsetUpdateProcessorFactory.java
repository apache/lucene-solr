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

import org.apache.solr.common.SolrInputField;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import static org.apache.solr.update.processor.FieldMutatingUpdateProcessor.mutator;

/**
 * Base class for processors that want to mutate selected fields to only 
 * keep a subset of the original values.
 * @see #pickSubset
 * @since 4.0.0
 */
public abstract class FieldValueSubsetUpdateProcessorFactory extends FieldMutatingUpdateProcessorFactory {

  @Override
  public final UpdateRequestProcessor getInstance(SolrQueryRequest req,
                                                  SolrQueryResponse rsp,
                                                  UpdateRequestProcessor next) {
    return mutator(getSelector(), next, src -> {
      if (src.getValueCount() <= 1) return src;

      SolrInputField result = new SolrInputField(src.getName());
      result.setValue(pickSubset(src.getValues()));
      return result;
    });
  }

  /**
   * Method subclasses must override to specify which values should be kept.  
   * This method will not be called unless the collection contains more then 
   * one value.
   */
  protected abstract Collection<Object> pickSubset(Collection<Object> values);
  
}

