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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.collation.ICUCollationKeyAnalyzer;
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

import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RuleBasedCollator;
import com.ibm.icu.util.ULocale;

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
 *  <li>locale: RFC 3066 locale ID (mandatory)
 *  <li>strength: 'primary','secondary','tertiary', 'quaternary', or 'identical' (optional)
 *  <li>decomposition: 'no', or 'canonical' (optional)
 * </ul>
 * <p>
 * Using a Tailored ruleset:
 * <ul>
 *  <li>custom: UTF-8 text file containing rules supported by RuleBasedCollator (mandatory)
 *  <li>strength: 'primary','secondary','tertiary', 'quaternary', or 'identical' (optional)
 *  <li>decomposition: 'no' or 'canonical' (optional)
 * </ul>
 * <p>
 * Expert options:
 * <ul>
 *  <li>alternate: 'shifted' or 'non-ignorable'. Can be used to ignore punctuation/whitespace.
 *  <li>caseLevel: 'true' or 'false'. Useful with strength=primary to ignore accents but not case.
 *  <li>caseFirst: 'lower' or 'upper'. Useful to control which is sorted first when case is not ignored.
 *  <li>numeric: 'true' or 'false'. Digits are sorted according to numeric value, e.g. foobar-9 sorts before foobar-10
 *  <li>variableTop: single character or contraction. Controls what is variable for 'alternate'
 * </ul>
 * 
 * @see Collator
 * @see ULocale
 * @see RuleBasedCollator
 */
public class ICUCollationField extends FieldType {
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
    String localeID = args.remove("locale");
    String strength = args.remove("strength");
    String decomposition = args.remove("decomposition");
    
    String alternate = args.remove("alternate");
    String caseLevel = args.remove("caseLevel");
    String caseFirst = args.remove("caseFirst");
    String numeric = args.remove("numeric");
    String variableTop = args.remove("variableTop");

    if (custom == null && localeID == null)
      throw new SolrException(ErrorCode.SERVER_ERROR, "Either custom or locale is required.");
    
    if (custom != null && localeID != null)
      throw new SolrException(ErrorCode.SERVER_ERROR, "Cannot specify both locale and custom. "
          + "To tailor rules for a built-in language, see the javadocs for RuleBasedCollator. "
          + "Then save the entire customized ruleset to a file, and use with the custom parameter");
    
    final Collator collator;
    
    if (localeID != null) { 
      // create from a system collator, based on Locale.
      collator = createFromLocale(localeID);
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
      else if (strength.equalsIgnoreCase("quaternary"))
        collator.setStrength(Collator.QUATERNARY);
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
      else
        throw new SolrException(ErrorCode.SERVER_ERROR, "Invalid decomposition: " + decomposition);
    }
    
    // expert options: concrete subclasses are always a RuleBasedCollator
    RuleBasedCollator rbc = (RuleBasedCollator) collator;
    if (alternate != null) {
      if (alternate.equalsIgnoreCase("shifted")) {
        rbc.setAlternateHandlingShifted(true);
      } else if (alternate.equalsIgnoreCase("non-ignorable")) {
        rbc.setAlternateHandlingShifted(false);
      } else {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Invalid alternate: " + alternate);
      }
    }
    if (caseLevel != null) {
      rbc.setCaseLevel(Boolean.parseBoolean(caseLevel));
    }
    if (caseFirst != null) {
      if (caseFirst.equalsIgnoreCase("lower")) {
        rbc.setLowerCaseFirst(true);
      } else if (caseFirst.equalsIgnoreCase("upper")) {
        rbc.setUpperCaseFirst(true);
      } else {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Invalid caseFirst: " + caseFirst);
      }
    }
    if (numeric != null) {
      rbc.setNumericCollation(Boolean.parseBoolean(numeric));
    }
    if (variableTop != null) {
      rbc.setVariableTop(variableTop);
    }

    analyzer = new ICUCollationKeyAnalyzer(collator);
  }
  
  /**
   * Create a locale from localeID.
   * Then return the appropriate collator for the locale.
   */
  private Collator createFromLocale(String localeID) {
    return Collator.getInstance(new ULocale(localeID));
  }
  
  /**
   * Read custom rules from a file, and create a RuleBasedCollator
   * The file cannot support comments, as # might be in the rules!
   */
  static Collator createFromRules(String fileName, ResourceLoader loader) {
    InputStream input = null;
    try {
     input = loader.openResource(fileName);
     String rules = IOUtils.toString(input, "UTF-8");
     return new RuleBasedCollator(rules);
    } catch (Exception e) {
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
   * analyze the text with the analyzer, instead of the collator.
   * because icu collators are not thread safe, this keeps things 
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
