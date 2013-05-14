package org.apache.lucene.analysis.synonym;

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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.synonym.SynonymFilter;
import org.apache.lucene.util.Version;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenFilterFactory;

/**
 * Factory for {@link SynonymFilter}.
 * <pre class="prettyprint" >
 * &lt;fieldType name="text_synonym" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.SynonymFilterFactory" synonyms="synonyms.txt" 
 *             format="solr" ignoreCase="false" expand="true" 
 *             tokenizerFactory="solr.WhitespaceTokenizerFactory"
 *             [optional tokenizer factory parameters]/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 * 
 * <p>
 * An optional param name prefix of "tokenizerFactory." may be used for any 
 * init params that the SynonymFilterFactory needs to pass to the specified 
 * TokenizerFactory.  If the TokenizerFactory expects an init parameters with 
 * the same name as an init param used by the SynonymFilterFactory, the prefix 
 * is mandatory.
 * </p>
 */
public class SynonymFilterFactory extends TokenFilterFactory implements ResourceLoaderAware {
  private final TokenFilterFactory delegator;

  public SynonymFilterFactory(Map<String,String> args) {
    super(args);
    assureMatchVersion();
    if (luceneMatchVersion.onOrAfter(Version.LUCENE_34)) {
      delegator = new FSTSynonymFilterFactory(new HashMap<String,String>(getOriginalArgs()));
    } else {
      // check if you use the new optional arg "format". this makes no sense for the old one, 
      // as its wired to solr's synonyms format only.
      if (args.containsKey("format") && !args.get("format").equals("solr")) {
        throw new IllegalArgumentException("You must specify luceneMatchVersion >= 3.4 to use alternate synonyms formats");
      }
      delegator = new SlowSynonymFilterFactory(new HashMap<String,String>(getOriginalArgs()));
    }
  }

  @Override
  public TokenStream create(TokenStream input) {
    return delegator.create(input);
  }

  @Override
  public void inform(ResourceLoader loader) throws IOException {
    ((ResourceLoaderAware) delegator).inform(loader);
  }

  /**
   * Access to the delegator TokenFilterFactory for test verification
   *
   * @deprecated Method exists only for testing 4x, will be removed in 5.0
   * @lucene.internal
   */
  TokenFilterFactory getDelegator() {
    return delegator;
  }
}
