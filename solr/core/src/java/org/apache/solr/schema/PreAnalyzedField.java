package org.apache.solr.schema;
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
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Constructor;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.GeneralField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.StorableField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.AttributeSource.State;
import org.apache.solr.analysis.SolrAnalyzer;
import org.apache.solr.response.TextResponseWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pre-analyzed field type provides a way to index a serialized token stream,
 * optionally with an independent stored value of a field.
 */
public class PreAnalyzedField extends FieldType {
  private static final Logger LOG = LoggerFactory.getLogger(PreAnalyzedField.class);

  /** Init argument name. Value is a fully-qualified class name of the parser
   * that implements {@link PreAnalyzedParser}.
   */
  public static final String PARSER_IMPL = "parserImpl";
  
  private static final String DEFAULT_IMPL = JsonPreAnalyzedParser.class.getName();

  
  private PreAnalyzedParser parser;
  
  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    super.init(schema, args);
    String implName = args.get(PARSER_IMPL);
    if (implName == null) {
      parser = new JsonPreAnalyzedParser();
    } else {
      try {
        Class<?> implClazz = Class.forName(implName);
        if (!PreAnalyzedParser.class.isAssignableFrom(implClazz)) {
          throw new Exception("must implement " + PreAnalyzedParser.class.getName());
        }
        Constructor<?> c = implClazz.getConstructor(new Class<?>[0]);
        parser = (PreAnalyzedParser) c.newInstance(new Object[0]);
      } catch (Exception e) {
        LOG.warn("Can't use the configured PreAnalyzedParser class '" + implName + "' (" +
            e.getMessage() + "), using default " + DEFAULT_IMPL);
        parser = new JsonPreAnalyzedParser();
      }
    }
  }

  @Override
  public Analyzer getAnalyzer() {
    return new SolrAnalyzer() {
      
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        return new TokenStreamComponents(new PreAnalyzedTokenizer(reader, parser));
      }
      
    };
  }
  
  @Override
  public Analyzer getQueryAnalyzer() {
    return getAnalyzer();
  }

  @Override
  public StorableField createField(SchemaField field, Object value,
          float boost) {
    StorableField f = null;
    try {
      f = fromString(field, String.valueOf(value), boost);
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
    return f;
  }

  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    return getStringSort(field, top);
  }

  @Override
  public void write(TextResponseWriter writer, String name, StorableField f)
          throws IOException {
    writer.writeStr(name, f.stringValue(), true);
  }
  
  /** Utility method to convert a field to a string that is parse-able by this
   * class.
   * @param f field to convert
   * @return string that is compatible with the serialization format
   * @throws IOException
   */
  public String toFormattedString(Field f) throws IOException {
    return parser.toFormattedString(f);
  }
  
  /**
   * This is a simple holder of a stored part and the collected states (tokens with attributes).
   */
  public static class ParseResult {
    public String str;
    public byte[] bin;
    public List<State> states = new LinkedList<State>();
  }
  
  /**
   * Parse the input and return the stored part and the tokens with attributes.
   */
  public static interface PreAnalyzedParser {
    /**
     * Parse input.
     * @param reader input to read from
     * @param parent parent who will own the resulting states (tokens with attributes)
     * @return parse result, with possibly null stored and/or states fields.
     * @throws IOException if a parsing error or IO error occurs
     */
    public ParseResult parse(Reader reader, AttributeSource parent) throws IOException;
    
    /**
     * Format a field so that the resulting String is valid for parsing with {@link #parse(Reader, AttributeSource)}.
     * @param f field instance
     * @return formatted string
     * @throws IOException
     */
    public String toFormattedString(Field f) throws IOException;
  }
  
  
  public StorableField fromString(SchemaField field, String val, float boost) throws Exception {
    if (val == null || val.trim().length() == 0) {
      return null;
    }
    PreAnalyzedTokenizer parse = new PreAnalyzedTokenizer(new StringReader(val), parser);
    parse.reset(); // consume
    Field f = (Field)super.createField(field, val, boost);
    if (parse.getStringValue() != null) {
      f.setStringValue(parse.getStringValue());
    } else if (parse.getBinaryValue() != null) {
      f.setBytesValue(parse.getBinaryValue());
    } else {
      f.fieldType().setStored(false);
    }
    
    if (parse.hasTokenStream()) {
      f.fieldType().setIndexed(true);
      f.fieldType().setTokenized(true);
      f.setTokenStream(parse);
    }
    return f;
  }
    
  /**
   * Token stream that works from a list of saved states.
   */
  private static class PreAnalyzedTokenizer extends Tokenizer {
    private final List<AttributeSource.State> cachedStates = new LinkedList<AttributeSource.State>();
    private Iterator<AttributeSource.State> it = null;
    private String stringValue = null;
    private byte[] binaryValue = null;
    private PreAnalyzedParser parser;
    private Reader lastReader;
    
    public PreAnalyzedTokenizer(Reader reader, PreAnalyzedParser parser) {
      super(reader);
      this.parser = parser;
    }
    
    public boolean hasTokenStream() {
      return !cachedStates.isEmpty();
    }
    
    public String getStringValue() {
      return stringValue;
    }
    
    public byte[] getBinaryValue() {
      return binaryValue;
    }
    
    public final boolean incrementToken() {
      // lazy init the iterator
      if (it == null) {
        it = cachedStates.iterator();
      }
    
      if (!it.hasNext()) {
        return false;
      }
      
      AttributeSource.State state = (State) it.next();
      restoreState(state.clone());
      return true;
    }
  
    @Override
    public final void reset() throws IOException {
      // NOTE: this acts like rewind if you call it again
      if (input != lastReader) {
        lastReader = input;
        cachedStates.clear();
        stringValue = null;
        binaryValue = null;
        ParseResult res = parser.parse(input, this);
        if (res != null) {
          stringValue = res.str;
          binaryValue = res.bin;
          if (res.states != null) {
            cachedStates.addAll(res.states);
          }
        }
      }
      it = cachedStates.iterator();
    }

    @Override
    public void close() throws IOException {
      super.close();
      lastReader = null; // just a ref, null for gc
    }
  }
  
}
