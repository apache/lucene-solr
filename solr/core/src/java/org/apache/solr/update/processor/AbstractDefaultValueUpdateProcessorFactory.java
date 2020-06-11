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

import org.apache.solr.common.SolrException;
import static org.apache.solr.common.SolrException.ErrorCode.*;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.update.AddUpdateCommand;

/**
 * <p>
 * Base class that can be extended by any
 * <code>UpdateRequestProcessorFactory</code> designed to add a default value 
 * to the document in an <code>AddUpdateCommand</code> when that field is not 
 * already specified.
 * </p>
 * <p>
 * This base class handles initialization of the <code>fieldName</code> init 
 * param, and provides an {@link AbstractDefaultValueUpdateProcessorFactory.DefaultValueUpdateProcessor} that Factory 
 * subclasses may choose to return from their <code>getInstance</code> 
 * implementation.
 * </p>
 * @since 4.0.0
 */
public abstract class AbstractDefaultValueUpdateProcessorFactory
  extends UpdateRequestProcessorFactory {

  protected String fieldName = null;

  @SuppressWarnings("unchecked")
  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {

    Object obj = args.remove("fieldName");
    if (null == obj && null == fieldName) {
      throw new SolrException
        (SERVER_ERROR, "'fieldName' init param must be specified and non-null"); 
    } else {
      fieldName = obj.toString();
    }

    if (0 < args.size()) {
      throw new SolrException(SERVER_ERROR, 
                              "Unexpected init param(s): '" + 
                              args.getName(0) + "'");
    }
    
    super.init(args);
  }

  /**
   * A simple processor that adds the results of {@link #getDefaultValue} 
   * to any document which does not already have a value in 
   * <code>fieldName</code>
   */
  static abstract class DefaultValueUpdateProcessor
    extends UpdateRequestProcessor {

    final String fieldName;

    public DefaultValueUpdateProcessor(final String fieldName,
                                       final UpdateRequestProcessor next) {
      super(next);
      this.fieldName = fieldName;
    }

    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException {
      final SolrInputDocument doc = cmd.getSolrInputDocument();

      if (! doc.containsKey(fieldName)) {
        doc.addField(fieldName, getDefaultValue());
      }

      super.processAdd(cmd);
    }
    
    public abstract Object getDefaultValue();
  }
}



