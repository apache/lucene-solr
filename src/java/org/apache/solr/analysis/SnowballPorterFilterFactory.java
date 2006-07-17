/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.analysis;

import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.solr.core.SolrCore;

/**
 * Factory for SnowballFilters, with configurable language
 * 
 * Browsing the code, SnowballFilter uses reflection to adapt to Lucene... don't
 * use this if you are concerned about speed. Use EnglishPorterFilterFactory.
 * 
 * @author yonik
 * @version $Id$
 */

public class SnowballPorterFilterFactory extends BaseTokenFilterFactory {
  private String language = "English";
  
  public void init(Map<String, String> args) {
    super.init(args);
    final String cfgLanguage = args.get("language");
    if(cfgLanguage!=null) language = cfgLanguage;
    SolrCore.log.fine("SnowballPorterFilterFactory: language=" + language);
  }
  
  public TokenStream create(TokenStream input) {
    return new SnowballFilter(input,language);
  }
}

