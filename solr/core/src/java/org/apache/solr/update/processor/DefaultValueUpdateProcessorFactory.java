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
import static org.apache.solr.common.SolrException.ErrorCode.*;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * <p>
 * An update processor that adds a constant default value to any document 
 * being added that does not already have a value in the specified field.
 * </p>
 *
 * <p>
 * In the example configuration below, if a document does not contain a value 
 * in the <code>price</code> and/or <code>type</code> fields, it will be given 
 * default values of <code>0.0</code> and/or <code>unknown</code> 
 * (respectively).
 * <br>
 *
 * <pre class="prettyprint">
 * &lt;processor class="solr.DefaultValueUpdateProcessorFactory"&gt;
 *   &lt;str name="fieldName"&gt;price&lt;/str&gt;
 *   &lt;float name="value"&gt;0.0&lt;/float&gt;
 * &lt;/processor&gt;
 * &lt;processor class="solr.DefaultValueUpdateProcessorFactory"&gt;
 *   &lt;str name="fieldName"&gt;type&lt;/str&gt;
 *   &lt;str name="value"&gt;unknown&lt;/str&gt;
 * &lt;/processor&gt;
 * </pre>
 * @since 4.0.0
 */
public class DefaultValueUpdateProcessorFactory
  extends AbstractDefaultValueUpdateProcessorFactory {

  protected Object defaultValue = null;

  @SuppressWarnings("unchecked")
  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {

    Object obj = args.remove("value");
    if (null == obj) {
      throw new SolrException
        (SERVER_ERROR, "'value' init param must be specified and non-null"); 
    } else {
      defaultValue = obj;
    }

    super.init(args);
  }

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req, 
                                            SolrQueryResponse rsp, 
                                            UpdateRequestProcessor next ) {
    return new DefaultValueUpdateProcessor(fieldName, next) {
      @Override
      public Object getDefaultValue() { return defaultValue; }
    };
  }

}



