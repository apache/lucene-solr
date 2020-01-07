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
package org.apache.solr.schema;

import java.io.IOException;
import java.io.InputStream;
import java.text.Collator;
import java.text.ParseException;
import java.text.RuleBasedCollator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.collation.CollationKeyAnalyzer;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrException;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.uninverting.UninvertingReader.Type;

/**
 * Field for collated sort keys. 
 * These can be used for locale-sensitive sort and range queries.
 * <p>
 * This field can be created in two ways: 
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
 * @see Collator
 * @see Locale
 * @see RuleBasedCollator
 * @since solr 4.0
 */
public class CollationField extends FieldType {
  private Analyzer analyzer;

  @Override
  protected void init(IndexSchema schema, Map<String,String> args) {
    properties |= TOKENIZED; // this ensures our analyzer gets hit
    setup(schema.getResourceLoader(), args);
    super.init(schema, args);
  }
  
  /**
   * Setup the field according to the provided parameters
   */
  private void setup(ResourceLoader loader, Map<String,String> args) {
    String custom = args.remove("custom");
    String language = args.remove("language");
    String country = args.remove("country");
    String variant = args.remove("variant");
    String strength = args.remove("strength");
    String decomposition = args.remove("decomposition");
    
    final Collator collator;

    if (custom == null && language == null)
      throw new SolrException(ErrorCode.SERVER_ERROR, "Either custom or language is required.");
    
    if (custom != null && 
        (language != null || country != null || variant != null))
      throw new SolrException(ErrorCode.SERVER_ERROR, "Cannot specify both language and custom. "
          + "To tailor rules for a built-in language, see the javadocs for RuleBasedCollator. "
          + "Then save the entire customized ruleset to a file, and use with the custom parameter");
    
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
        throw new SolrException(ErrorCode.SERVER_ERROR, "Invalid strength: " + strength);
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
        throw new SolrException(ErrorCode.SERVER_ERROR, "Invalid decomposition: " + decomposition);
    }
    analyzer = new CollationKeyAnalyzer(collator);
  }
  
  /**
   * Create a locale from language, with optional country and variant.
   * Then return the appropriate collator for the locale.
   */
  private Collator createFromLocale(String language, String country, String variant) {
    Locale locale;
    
    if (language != null && country == null && variant != null)
      throw new SolrException(ErrorCode.SERVER_ERROR, 
          "To specify variant, country is required");
    else if (language != null && country != null && variant != null)
      locale = new Locale(language, country, variant);
    else if (language != null && country != null)
      locale = new Locale(language, country);
    else 
      locale = new Locale(language);
    
    return Collator.getInstance(locale);
  }
  
  /**
   * Read custom rules from a file, and create a RuleBasedCollator
   * The file cannot support comments, as # might be in the rules!
   */
  private Collator createFromRules(String fileName, ResourceLoader loader) {
    InputStream input = null;
    try {
     input = loader.openResource(fileName);
     String rules = IOUtils.toString(input, "UTF-8");
     return new RuleBasedCollator(rules);
    } catch (IOException | ParseException e) {
      // io error or invalid rules
      throw new RuntimeException(e);
    } finally {
      IOUtils.closeQuietly(input);
    }
  }

  @Override
  public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
    writer.writeStr(name, f.stringValue(), true);
  }

  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    return getStringSort(field, top);
  }
  
  @Override
  public Type getUninversionType(SchemaField sf) {
    if (sf.multiValued()) {
      return Type.SORTED_SET_BINARY;
    } else {
      return Type.SORTED;
    }
  }

  @Override
  public Analyzer getIndexAnalyzer() {
    return analyzer;
  }

  @Override
  public Analyzer getQueryAnalyzer() {
    return analyzer;
  }

  /**
   * analyze the range with the analyzer, instead of the collator.
   * because jdk collators might not be thread safe (when they are
   * it's just that all methods are synced), this keeps things 
   * simple (we already have a threadlocal clone in the reused TS)
   */
  private BytesRef getCollationKey(String field, String text) {     
    try (TokenStream source = analyzer.tokenStream(field, text)) {
      source.reset();    
      TermToBytesRefAttribute termAtt = source.getAttribute(TermToBytesRefAttribute.class);

      // we control the analyzer here: most errors are impossible
      if (!source.incrementToken())
        throw new IllegalArgumentException("analyzer returned no terms for text: " + text);
      BytesRef bytes = BytesRef.deepCopyOf(termAtt.getBytesRef());
      assert !source.incrementToken();
      source.end();
      return bytes;
    } catch (IOException e) {
      throw new RuntimeException("Unable to analyze text: " + text, e);
    }
  }
  
  @Override
  protected Query getSpecializedRangeQuery(QParser parser, SchemaField field, String part1, String part2, boolean minInclusive, boolean maxInclusive) {
    String f = field.getName();
    BytesRef low = part1 == null ? null : getCollationKey(f, part1);
    BytesRef high = part2 == null ? null : getCollationKey(f, part2);
    if (!field.indexed() && field.hasDocValues()) {
      return SortedSetDocValuesField.newSlowRangeQuery(
          field.getName(), low, high, minInclusive, maxInclusive);
    } else {
      return new TermRangeQuery(field.getName(), low, high, minInclusive, maxInclusive);
    }
  }

  @Override
  protected void checkSupportsDocValues() { // we support DocValues
  }

  @Override
  public List<IndexableField> createFields(SchemaField field, Object value) {
    if (field.hasDocValues()) {
      List<IndexableField> fields = new ArrayList<>();
      fields.add(createField(field, value));
      final BytesRef bytes = getCollationKey(field.getName(), value.toString());
      if (field.multiValued()) {
        fields.add(new SortedSetDocValuesField(field.getName(), bytes));
      } else {
        fields.add(new SortedDocValuesField(field.getName(), bytes));
      }
      return fields;
    } else {
      return Collections.singletonList(createField(field, value));
    }
  }

  @Override
  public Object marshalSortValue(Object value) {
    return marshalBase64SortValue(value);
  }

  @Override
  public Object unmarshalSortValue(Object value) {
    return unmarshalBase64SortValue(value);
  }
}
