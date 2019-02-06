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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.util.plugin.SolrCoreAware;

import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;

/**
 * Identifies the language of a set of input fields using 
 * http://code.google.com/p/language-detection
 * <p>
 * The UpdateProcessorChain config entry can take a number of parameters
 * which may also be passed as HTTP parameters on the update request
 * and override the defaults. Here is the simplest processor config possible:
 * 
 * <pre class="prettyprint" >
 * &lt;processor class=&quot;org.apache.solr.update.processor.LangDetectLanguageIdentifierUpdateProcessorFactory&quot;&gt;
 *   &lt;str name=&quot;langid.fl&quot;&gt;title,text&lt;/str&gt;
 *   &lt;str name=&quot;langid.langField&quot;&gt;language_s&lt;/str&gt;
 * &lt;/processor&gt;
 * </pre>
 * See <a href="http://wiki.apache.org/solr/LanguageDetection">http://wiki.apache.org/solr/LanguageDetection</a>
 * @since 3.5
 */
public class LangDetectLanguageIdentifierUpdateProcessorFactory extends
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
    try {
      loadData();
    } catch (Exception e) {
      throw new RuntimeException("Couldn't load profile data, will return empty languages always!", e);
    }
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
    return new LangDetectLanguageIdentifierUpdateProcessor(req, rsp, next);
  }
  
  
  // DetectorFactory is totally global, so we only want to do this once... ever!!!
  static boolean loaded;
  
  // profiles we will load from classpath
  static final String languages[] = {
    "af", "ar", "bg", "bn", "cs", "da", "de", "el", "en", "es", "et", "fa", "fi", "fr", "gu",
    "he", "hi", "hr", "hu", "id", "it", "ja", "kn", "ko", "lt", "lv", "mk", "ml", "mr", "ne",
    "nl", "no", "pa", "pl", "pt", "ro", "ru", "sk", "sl", "so", "sq", "sv", "sw", "ta", "te",
    "th", "tl", "tr", "uk", "ur", "vi", "zh-cn", "zh-tw"
  };

  public static synchronized void loadData() throws IOException, LangDetectException {
    if (loaded) {
      return;
    }
    loaded = true;
    List<String> profileData = new ArrayList<>();
    for (String language : languages) {
      InputStream stream = LangDetectLanguageIdentifierUpdateProcessor.class.getResourceAsStream("langdetect-profiles/" + language);
      BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
      profileData.add(new String(IOUtils.toCharArray(reader)));
      reader.close();
    }
    DetectorFactory.loadProfile(profileData);
    DetectorFactory.setSeed(0);
  }
}
