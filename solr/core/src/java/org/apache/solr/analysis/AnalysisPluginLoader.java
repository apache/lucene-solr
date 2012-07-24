package org.apache.solr.analysis;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.util.AbstractAnalysisFactory;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.solr.util.plugin.AbstractPluginLoader;
import org.w3c.dom.Node;

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

public abstract class AnalysisPluginLoader<S extends AbstractAnalysisFactory> extends AbstractPluginLoader<S> {
  
  public AnalysisPluginLoader(String type, Class<S> pluginClassType, boolean preRegister, boolean requireName) {
    super(type, pluginClassType, preRegister, requireName);
  }

  public AnalysisPluginLoader(String type, Class<S> pluginClassType) {
    super(type, pluginClassType);
  }

  @Override
  protected S create(ResourceLoader loader, String name, String className, Node node) throws Exception {
    S instance = null;
    Matcher m = legacyPattern.matcher(className);
    if (m.matches()) {
      try {
        instance = createSPI(m.group(4));
      } catch (IllegalArgumentException ex) { 
        // ok
      }
    }
    
    if (instance != null) {
      // necessary because SolrResourceLoader manages its own list of 'awaiting ResourceLoaderAware'
      className = instance.getClass().getName();
    }
    
    return super.create(loader, name, className, node);
  }
  
  private static final Pattern legacyPattern = 
      Pattern.compile("((org\\.apache\\.solr\\.analysis\\.)|(solr\\.))([\\p{L}_$][\\p{L}\\p{N}_$]+?)(TokenFilter|Filter|Tokenizer|CharFilter)Factory");
  
  protected abstract S createSPI(String name);
}
