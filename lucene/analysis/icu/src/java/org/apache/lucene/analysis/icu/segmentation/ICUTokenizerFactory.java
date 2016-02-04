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
package org.apache.lucene.analysis.icu.segmentation;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.util.AttributeFactory;
import org.apache.lucene.util.IOUtils;

import com.ibm.icu.lang.UCharacter;
import com.ibm.icu.lang.UProperty;
import com.ibm.icu.lang.UScript;
import com.ibm.icu.text.BreakIterator;
import com.ibm.icu.text.RuleBasedBreakIterator;

/**
 * Factory for {@link ICUTokenizer}.
 * Words are broken across script boundaries, then segmented according to
 * the BreakIterator and typing provided by the {@link DefaultICUTokenizerConfig}.
 *
 * <p>
 * To use the default set of per-script rules:
 *
 * <pre class="prettyprint" >
 * &lt;fieldType name="text_icu" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.ICUTokenizerFactory"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 * <p>
 * You can customize this tokenizer's behavior by specifying per-script rule files,
 * which are compiled by the ICU RuleBasedBreakIterator.  See the
 * <a href="http://userguide.icu-project.org/boundaryanalysis#TOC-RBBI-Rules"
 * >ICU RuleBasedBreakIterator syntax reference</a>.
 *
 * <p>
 * To add per-script rules, add a "rulefiles" argument, which should contain a
 * comma-separated list of <tt>code:rulefile</tt> pairs in the following format:
 * <a href="http://unicode.org/iso15924/iso15924-codes.html"
 * >four-letter ISO 15924 script code</a>, followed by a colon, then a resource
 * path.  E.g. to specify rules for Latin (script code "Latn") and Cyrillic
 * (script code "Cyrl"):
 *
 * <pre class="prettyprint" >
 * &lt;fieldType name="text_icu_custom" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.ICUTokenizerFactory" cjkAsWords="true"
 *                rulefiles="Latn:my.Latin.rules.rbbi,Cyrl:my.Cyrillic.rules.rbbi"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 */
public class ICUTokenizerFactory extends TokenizerFactory implements ResourceLoaderAware {
  static final String RULEFILES = "rulefiles";
  private final Map<Integer,String> tailored;
  private ICUTokenizerConfig config;
  private final boolean cjkAsWords;
  
  /** Creates a new ICUTokenizerFactory */
  public ICUTokenizerFactory(Map<String,String> args) {
    super(args);
    tailored = new HashMap<>();
    String rulefilesArg = get(args, RULEFILES);
    if (rulefilesArg != null) {
      List<String> scriptAndResourcePaths = splitFileNames(rulefilesArg);
      for (String scriptAndResourcePath : scriptAndResourcePaths) {
        int colonPos = scriptAndResourcePath.indexOf(":");
        String scriptCode = scriptAndResourcePath.substring(0, colonPos).trim();
        String resourcePath = scriptAndResourcePath.substring(colonPos+1).trim();
        tailored.put(UCharacter.getPropertyValueEnum(UProperty.SCRIPT, scriptCode), resourcePath);
      }
    }
    cjkAsWords = getBoolean(args, "cjkAsWords", true);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public void inform(ResourceLoader loader) throws IOException {
    assert tailored != null : "init must be called first!";
    if (tailored.isEmpty()) {
      config = new DefaultICUTokenizerConfig(cjkAsWords);
    } else {
      final BreakIterator breakers[] = new BreakIterator[UScript.CODE_LIMIT];
      for (Map.Entry<Integer,String> entry : tailored.entrySet()) {
        int code = entry.getKey();
        String resourcePath = entry.getValue();
        breakers[code] = parseRules(resourcePath, loader);
      }
      config = new DefaultICUTokenizerConfig(cjkAsWords) {
        
        @Override
        public BreakIterator getBreakIterator(int script) {
          if (breakers[script] != null) {
            return (BreakIterator) breakers[script].clone();
          } else {
            return super.getBreakIterator(script);
          }
        }
        // TODO: we could also allow codes->types mapping
      };
    }
  }
  
  private BreakIterator parseRules(String filename, ResourceLoader loader) throws IOException {
    StringBuilder rules = new StringBuilder();
    InputStream rulesStream = loader.openResource(filename);
    BufferedReader reader = new BufferedReader
        (IOUtils.getDecodingReader(rulesStream, StandardCharsets.UTF_8));
    String line = null;
    while ((line = reader.readLine()) != null) {
      if ( ! line.startsWith("#"))
        rules.append(line);
      rules.append('\n');
    }
    reader.close();
    return new RuleBasedBreakIterator(rules.toString());
  }

  @Override
  public ICUTokenizer create(AttributeFactory factory) {
    assert config != null : "inform must be called first!";
    return new ICUTokenizer(factory, config);
  }
}
