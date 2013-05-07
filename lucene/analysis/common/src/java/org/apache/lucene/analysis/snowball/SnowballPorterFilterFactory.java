package org.apache.lucene.analysis.snowball;

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

import java.util.Map;
import java.io.IOException;

import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.tartarus.snowball.SnowballProgram;

/**
 * Factory for {@link SnowballFilter}, with configurable language
 * <p>
 * Note: Use of the "Lovins" stemmer is not recommended, as it is implemented with reflection.
 * <pre class="prettyprint">
 * &lt;fieldType name="text_snowballstem" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.StandardTokenizerFactory"/&gt;
 *     &lt;filter class="solr.LowerCaseFilterFactory"/&gt;
 *     &lt;filter class="solr.SnowballPorterFilterFactory" protected="protectedkeyword.txt" language="English"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 */
public class SnowballPorterFilterFactory extends TokenFilterFactory implements ResourceLoaderAware {
  public static final String PROTECTED_TOKENS = "protected";

  private final String language;
  private final String wordFiles;
  private Class<? extends SnowballProgram> stemClass;
  private CharArraySet protectedWords = null;
  
  /** Creates a new SnowballPorterFilterFactory */
  public SnowballPorterFilterFactory(Map<String,String> args) {
    super(args);
    language = get(args, "language", "English");
    wordFiles = get(args, PROTECTED_TOKENS);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public void inform(ResourceLoader loader) throws IOException {
    String className = "org.tartarus.snowball.ext." + language + "Stemmer";
    stemClass = loader.newInstance(className, SnowballProgram.class).getClass();

    if (wordFiles != null) {
      protectedWords = getWordSet(loader, wordFiles, false);
    }
  }

  @Override
  public TokenFilter create(TokenStream input) {
    SnowballProgram program;
    try {
      program = stemClass.newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Error instantiating stemmer for language " + language + "from class " + stemClass, e);
    }

    if (protectedWords != null)
      input = new SetKeywordMarkerFilter(input, protectedWords);
    return new SnowballFilter(input, program);
  }
}

