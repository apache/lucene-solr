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
package org.apache.lucene.analysis.core;


import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Factory class for {@link TypeTokenFilter}.
 * <pre class="prettyprint">
 * &lt;fieldType name="chars" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.StandardTokenizerFactory"/&gt;
 *     &lt;filter class="solr.TypeTokenFilterFactory" types="stoptypes.txt"
 *                   useWhitelist="false"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 */
public class TypeTokenFilterFactory extends TokenFilterFactory implements ResourceLoaderAware {
  private final boolean useWhitelist;
  private final String stopTypesFiles;
  private Set<String> stopTypes;
  private boolean enablePositionIncrements;
  
  /** Creates a new TypeTokenFilterFactory */
  public TypeTokenFilterFactory(Map<String,String> args) {
    super(args);
    stopTypesFiles = require(args, "types");
    useWhitelist = getBoolean(args, "useWhitelist", false);

    if (luceneMatchVersion.onOrAfter(Version.LUCENE_5_0_0) == false) {
      boolean defaultValue = luceneMatchVersion.onOrAfter(Version.LUCENE_4_4_0);
      enablePositionIncrements = getBoolean(args, "enablePositionIncrements", defaultValue);
      if (enablePositionIncrements == false && luceneMatchVersion.onOrAfter(Version.LUCENE_4_4_0)) {
        throw new IllegalArgumentException("enablePositionIncrements=false is not supported anymore as of Lucene 4.4");
      }
    } else if (args.containsKey("enablePositionIncrements")) {
      throw new IllegalArgumentException("enablePositionIncrements is not a valid option as of Lucene 5.0");
    }
    
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }
  
  @Override
  public void inform(ResourceLoader loader) throws IOException {
    List<String> files = splitFileNames(stopTypesFiles);
    if (files.size() > 0) {
      stopTypes = new HashSet<>();
      for (String file : files) {
        List<String> typesLines = getLines(loader, file.trim());
        stopTypes.addAll(typesLines);
      }
    }
  }

  public Set<String> getStopTypes() {
    return stopTypes;
  }

  @Override
  public TokenStream create(TokenStream input) {
    if (luceneMatchVersion.onOrAfter(Version.LUCENE_4_4_0)) {
      return new TypeTokenFilter(input, stopTypes, useWhitelist);
    } else {
      @SuppressWarnings("deprecation")
      final TokenStream filter = new Lucene43TypeTokenFilter(enablePositionIncrements, input, stopTypes, useWhitelist);
      return filter;
    }
  }
}
