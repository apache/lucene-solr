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

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.util.plugin.SolrCoreAware;

/**
 * Identifies the language of a set of input fields using Tika's
 * LanguageIdentifier. The tika-core-x.y.jar must be on the classpath
 * <p>
 * The UpdateProcessorChain config entry can take a number of parameters
 * which may also be passed as HTTP parameters on the update request
 * and override the defaults. Here is the simplest processor config possible:
 * 
 * <pre class="prettyprint" >
 * &lt;processor class=&quot;org.apache.solr.update.processor.TikaLanguageIdentifierUpdateProcessorFactory&quot;&gt;
 *   &lt;str name=&quot;langid.fl&quot;&gt;title,text&lt;/str&gt;
 *   &lt;str name=&quot;langid.langField&quot;&gt;language_s&lt;/str&gt;
 * &lt;/processor&gt;
 * </pre>
 * See <a href="http://wiki.apache.org/solr/LanguageDetection">http://wiki.apache.org/solr/LanguageDetection</a>
 * @since 3.5
 */
public class TikaLanguageIdentifierUpdateProcessorFactory extends
        UpdateRequestProcessorFactory implements SolrCoreAware, LangIdParams {

  protected SolrParams defaults;
  protected SolrParams appends;
  protected SolrParams invariants;

  @Override
  public void inform(SolrCore core) {
  }

  /**
   * The UpdateRequestProcessor may be initialized in solrconfig.xml similarly
   * to a RequestHandler, with defaults, appends and invariants.
   * @param args a NamedList with the configuration parameters 
   */
  @Override
  @SuppressWarnings("rawtypes")
  public void init( NamedList args )
  {
    if (args != null) {
      Object o;
      o = args.get("defaults");
      if (o != null && o instanceof NamedList) {
        defaults = ((NamedList) o).toSolrParams();
      } else {
        defaults = args.toSolrParams();
      }
      o = args.get("appends");
      if (o != null && o instanceof NamedList) {
        appends = ((NamedList) o).toSolrParams();
      }
      o = args.get("invariants");
      if (o != null && o instanceof NamedList) {
        invariants = ((NamedList) o).toSolrParams();
      }
    }
  }

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req,
                                            SolrQueryResponse rsp, UpdateRequestProcessor next) {
    // Process defaults, appends and invariants if we got a request
    if(req != null) {
      SolrPluginUtils.setDefaults(req, defaults, appends, invariants);
    }
    return new TikaLanguageIdentifierUpdateProcessor(req, rsp, next);
  }


}
