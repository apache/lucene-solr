package org.apache.lucene.analysis.core;

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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.TypeTokenFilter;
import org.apache.lucene.analysis.util.InitializationException;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenFilterFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Factory class for {@link TypeTokenFilter}.
 * <pre class="prettyprint" >
 * &lt;fieldType name="chars" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.StandardTokenizerFactory"/&gt;
 *     &lt;filter class="solr.TypeTokenFilterFactory" types="stoptypes.txt"
 *                   enablePositionIncrements="true" useWhiteList="false"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 */
public class TypeTokenFilterFactory extends TokenFilterFactory implements ResourceLoaderAware {

  @Override
  public void inform(ResourceLoader loader) {
    String stopTypesFiles = args.get("types");
    enablePositionIncrements = getBoolean("enablePositionIncrements", false);
    useWhitelist = getBoolean("useWhitelist", false);
    if (stopTypesFiles != null) {
      try {
        List<String> files = splitFileNames(stopTypesFiles);
        if (files.size() > 0) {
          stopTypes = new HashSet<String>();
          for (String file : files) {
            List<String> typesLines = loader.getLines(file.trim());
            stopTypes.addAll(typesLines);
          }
        }
      } catch (IOException e) {
        throw new InitializationException("IOException thrown while loading types", e);
      }
    } else {
      throw new InitializationException("Missing required parameter: types.");
    }
  }

  private boolean useWhitelist;
  private Set<String> stopTypes;
  private boolean enablePositionIncrements;

  public boolean isEnablePositionIncrements() {
    return enablePositionIncrements;
  }

  public Set<String> getStopTypes() {
    return stopTypes;
  }

  @Override
  public TokenStream create(TokenStream input) {
    return new TypeTokenFilter(enablePositionIncrements, input, stopTypes, useWhitelist);
  }
}
