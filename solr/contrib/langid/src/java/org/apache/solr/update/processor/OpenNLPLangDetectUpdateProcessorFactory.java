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
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.util.plugin.SolrCoreAware;

import opennlp.tools.langdetect.LanguageDetectorModel;

/**
 * Identifies the language of a set of input fields using <a href="https://opennlp.apache.org/">Apache OpenNLP</a>.
 * <p>
 * The UpdateProcessorChain config entry can take a number of parameters
 * which may also be passed as HTTP parameters on the update request
 * and override the defaults. Here is the simplest processor config possible:
 * 
 * <pre class="prettyprint" >
 * &lt;processor class=&quot;org.apache.solr.update.processor.OpenNLPLangDetectUpdateProcessorFactory&quot;&gt;
 *   &lt;str name=&quot;langid.fl&quot;&gt;title,text&lt;/str&gt;
 *   &lt;str name=&quot;langid.langField&quot;&gt;language_s&lt;/str&gt;
 *   &lt;str name="langid.model"&gt;langdetect-183.bin&lt;/str&gt;
 * &lt;/processor&gt;
 * </pre>
 * See <a href="http://wiki.apache.org/solr/LanguageDetection">http://wiki.apache.org/solr/LanguageDetection</a>
 *
 * @since 7.3.0
 */
public class OpenNLPLangDetectUpdateProcessorFactory extends UpdateRequestProcessorFactory
  implements SolrCoreAware {

  private static final String MODEL_PARAM = "langid.model";
  private String modelFile;
  private LanguageDetectorModel model;
  protected SolrParams defaults;
  protected SolrParams appends;
  protected SolrParams invariants;
  private SolrResourceLoader solrResourceLoader;

  @SuppressWarnings("rawtypes")
  @Override
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

      // Look for model filename in invariants, then in args, then defaults
      if (invariants != null) {
        modelFile = invariants.get(MODEL_PARAM);
      }
      if (modelFile == null) {
        o = args.get(MODEL_PARAM);
        if (o != null && o instanceof String) {
          modelFile = (String)o;
        } else {
          modelFile = defaults.get(MODEL_PARAM);
          if (modelFile == null) {
            throw new RuntimeException("Couldn't load language model, will return empty languages always!");
          }
        }
      }
    }
  }

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    // Process defaults, appends and invariants if we got a request
    if (req != null) {
      SolrPluginUtils.setDefaults(req, defaults, appends, invariants);
    }
    return new OpenNLPLangDetectUpdateProcessor(req, rsp, next, model);
  }

  @SuppressWarnings("deprecation")
  private void loadModel() throws IOException {
    InputStream is = null;
    try{
      if (modelFile != null) {
        is = solrResourceLoader.openResource(modelFile);
        model = new LanguageDetectorModel(is);
      }
    }
    finally{
      IOUtils.closeQuietly(is);
    }
  }

  @Override
  public void inform(SolrCore core){
    solrResourceLoader = core.getResourceLoader();
    try {
      loadModel();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
