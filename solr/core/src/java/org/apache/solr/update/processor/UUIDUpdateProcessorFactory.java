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
import java.util.UUID;
import java.util.Locale;

import org.apache.solr.common.SolrException;
import static org.apache.solr.common.SolrException.ErrorCode.*;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;


/**
 * <p>
 * An update processor that adds a newly generated <code>UUID</code> value 
 * to any document being added that does not already have a value in the 
 * specified field.
 * </p>
 *
 * <p>
 * In the example configuration below, if a document does not contain a value 
 * in the <code>id</code> field, a new <code>UUID</code> will be generated 
 * and added as the value of that field.
 * <p>
 *
 * <pre class="prettyprint">
 * &lt;processor class="solr.UUIDUpdateProcessorFactory"&gt;
 *   &lt;str name="fieldName"&gt;id&lt;/str&gt;
 * &lt;/processor&gt;
 * </pre>
 * 
 * @see UUID
 */
public class UUIDUpdateProcessorFactory
  extends AbstractDefaultValueUpdateProcessorFactory {

  public UpdateRequestProcessor getInstance(SolrQueryRequest req, 
                                            SolrQueryResponse rsp, 
                                            UpdateRequestProcessor next ) {
    return new DefaultValueUpdateProcessor(fieldName, next) {
      public Object getDefaultValue() { 
        return UUID.randomUUID().toString().toLowerCase(Locale.ENGLISH);
      }
    };
  }
}



