package org.apache.lucene.collation;

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
import java.io.InputStream;
import java.io.Reader;
import java.text.Collator;
import java.text.ParseException;
import java.text.RuleBasedCollator;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.*;
import org.apache.lucene.collation.CollationKeyFilter;
import org.apache.lucene.util.IOUtils;

/**
 * Factory for {@link CollationKeyFilter}.
 * <p>
 * This factory can be created in two ways: 
 * <ul>
 *  <li>Based upon a system collator associated with a Locale.
 *  <li>Based upon a tailored ruleset.
 * </ul>
 * <p>
 * Using a System collator:
 * <ul>
 *  <li>language: ISO-639 language code (mandatory)
 *  <li>country: ISO-3166 country code (optional)
 *  <li>variant: vendor or browser-specific code (optional)
 *  <li>strength: 'primary','secondary','tertiary', or 'identical' (optional)
 *  <li>decomposition: 'no','canonical', or 'full' (optional)
 * </ul>
 * <p>
 * Using a Tailored ruleset:
 * <ul>
 *  <li>custom: UTF-8 text file containing rules supported by RuleBasedCollator (mandatory)
 *  <li>strength: 'primary','secondary','tertiary', or 'identical' (optional)
 *  <li>decomposition: 'no','canonical', or 'full' (optional)
 * </ul>
 * 
 * <pre class="prettyprint" >
 * &lt;fieldType name="text_clltnky" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.KeywordTokenizerFactory"/&gt;
 *     &lt;filter class="solr.CollationKeyFilterFactory" language="ja" country="JP"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 * 
 * @see Collator
 * @see Locale
 * @see RuleBasedCollator
 * @since solr 3.1
 * @deprecated use {@link CollationKeyAnalyzer} instead.
 */
@Deprecated
public class CollationKeyFilterFactory extends TokenFilterFactory implements MultiTermAwareComponent, ResourceLoaderAware {
  private Collator collator;
  private final String custom;
  private final String language;
  private final String country;
  private final String variant;
  private final String strength;
  private final String decomposition;
  
  public CollationKeyFilterFactory(Map<String,String> args) {
    super(args);
    custom = args.remove("custom");
    language = args.remove("language");
    country = args.remove("country");
    variant = args.remove("variant");
    strength = args.remove("strength");
    decomposition = args.remove("decomposition");
    
    if (custom == null && language == null)
      throw new IllegalArgumentException("Either custom or language is required.");
    
    if (custom != null && 
        (language != null || country != null || variant != null))
      throw new IllegalArgumentException("Cannot specify both language and custom. "
          + "To tailor rules for a built-in language, see the javadocs for RuleBasedCollator. "
          + "Then save the entire customized ruleset to a file, and use with the custom parameter");
    
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  public void inform(ResourceLoader loader) throws IOException {
    if (language != null) { 
      // create from a system collator, based on Locale.
      collator = createFromLocale(language, country, variant);
    } else { 
      // create from a custom ruleset
      collator = createFromRules(custom, loader);
    }
    
    // set the strength flag, otherwise it will be the default.
    if (strength != null) {
      if (strength.equalsIgnoreCase("primary"))
        collator.setStrength(Collator.PRIMARY);
      else if (strength.equalsIgnoreCase("secondary"))
        collator.setStrength(Collator.SECONDARY);
      else if (strength.equalsIgnoreCase("tertiary"))
        collator.setStrength(Collator.TERTIARY);
      else if (strength.equalsIgnoreCase("identical"))
        collator.setStrength(Collator.IDENTICAL);
      else
        throw new IllegalArgumentException("Invalid strength: " + strength);
    }
    
    // set the decomposition flag, otherwise it will be the default.
    if (decomposition != null) {
      if (decomposition.equalsIgnoreCase("no"))
        collator.setDecomposition(Collator.NO_DECOMPOSITION);
      else if (decomposition.equalsIgnoreCase("canonical"))
        collator.setDecomposition(Collator.CANONICAL_DECOMPOSITION);
      else if (decomposition.equalsIgnoreCase("full"))
        collator.setDecomposition(Collator.FULL_DECOMPOSITION);
      else
        throw new IllegalArgumentException("Invalid decomposition: " + decomposition);
    }
  }
  
  public TokenStream create(TokenStream input) {
    return new CollationKeyFilter(input, collator);
  }
  
  /*
   * Create a locale from language, with optional country and variant.
   * Then return the appropriate collator for the locale.
   */
  private Collator createFromLocale(String language, String country, String variant) {
    Locale locale;
    
    if (language != null && country == null && variant != null)
      throw new IllegalArgumentException("To specify variant, country is required");
    else if (language != null && country != null && variant != null)
      locale = new Locale(language, country, variant);
    else if (language != null && country != null)
      locale = new Locale(language, country);
    else 
      locale = new Locale(language);
    
    return Collator.getInstance(locale);
  }
  
  /*
   * Read custom rules from a file, and create a RuleBasedCollator
   * The file cannot support comments, as # might be in the rules!
   */
  private Collator createFromRules(String fileName, ResourceLoader loader) throws IOException {
    InputStream input = null;
    try {
     input = loader.openResource(fileName);
     String rules = toUTF8String(input);
     return new RuleBasedCollator(rules);
    } catch (ParseException e) {
      // invalid rules
      throw new IOException("ParseException thrown while parsing rules", e);
    } finally {
      IOUtils.closeWhileHandlingException(input);
    }
  }
  
  @Override
  public AbstractAnalysisFactory getMultiTermComponent() {
    return this;
  }
  
  private String toUTF8String(InputStream in) throws IOException {
    StringBuilder sb = new StringBuilder();
    char buffer[] = new char[1024];
    Reader r = IOUtils.getDecodingReader(in, IOUtils.CHARSET_UTF_8);
    int len = 0;
    while ((len = r.read(buffer)) > 0) {
      sb.append(buffer, 0, len);
    }
    return sb.toString();
  }
}
