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

package org.apache.lucene.luwak;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;

/**
 * An InputDocument represents a document to be run against registered queries
 * in the Monitor.  It should be constructed using the static #builder() method.
 */
public class InputDocument {

  private static final FieldType FIELD_TYPE = new FieldType();

  static {
    FIELD_TYPE.setStored(false);
    FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
  }

  /**
   * The field name luwak uses for its internal id
   */
  public static final String ID_FIELD = "_luwak_id";

  /**
   * Create a new fluent {@link InputDocument.Builder} object.
   *
   * @param id the id
   * @return a Builder
   */
  public static Builder builder(String id) {
    return new Builder(id);
  }

  private final String id;
  private final Document luceneDocument;
  private final PerFieldAnalyzerWrapper analyzers;

  // protected constructor - use a Builder to create objects
  protected InputDocument(String id, Document luceneDocument, PerFieldAnalyzerWrapper analyzers) {
    this.id = id;
    this.luceneDocument = luceneDocument;
    this.analyzers = analyzers;
  }

  /**
   * Get the document's ID
   *
   * @return the document's ID
   */
  public String getId() {
    return id;
  }

  /**
   * @return a representation of this InputDocument as a lucene {@link Document}
   */
  public Document getDocument() {
    return luceneDocument;
  }

  /**
   * @return a {@link PerFieldAnalyzerWrapper} describing how the different fields will be analysed
   */
  public PerFieldAnalyzerWrapper getAnalyzers() {
    return analyzers;
  }

  /**
   * Fluent interface to construct a new InputDocument
   */
  public static class Builder {

    private final String id;
    private final Document doc = new Document();
    private Map<String, Analyzer> analyzers = new HashMap<>();
    private Analyzer defaultAnalyzer = new KeywordAnalyzer();

    /**
     * Create a new Builder for an InputDocument with the given id
     *
     * @param id the id of the InputDocument
     */
    public Builder(String id) {
      this.id = id;
    }

    public Builder setDefaultAnalyzer(Analyzer analyzer) {
      this.defaultAnalyzer = analyzer;
      return this;
    }

    /**
     * Add a text field to the InputDocument
     * <p>
     * Positions and Offsets are recorded, but the field value is not stored.
     *
     * @param field    the field name
     * @param text     the text content of the field
     * @param analyzer the {@link Analyzer} that should be used to analyse this field
     * @return the Builder object
     * <p>
     * N.B. Analysis is not actually run until this InputDocument is added to a {@link DocumentBatch},
     * so if this method is called multiple times for the same field but with different Analyzers, the
     * last Analyzer to be passed in will be used to analyze all of the values.
     */
    public Builder addField(String field, String text, Analyzer analyzer) {
      checkFieldName(field);
      doc.add(new Field(field, text, FIELD_TYPE));
      analyzers.put(field, analyzer);
      return this;
    }

    /**
     * Add a field to the InputDocument
     *
     * @param field the field name
     * @param ts    a {@link TokenStream} containing token values for this field
     * @return the Builder object
     */
    public Builder addField(String field, TokenStream ts) {
      checkFieldName(field);
      doc.add(new Field(field, ts, FIELD_TYPE));
      return this;
    }

    /**
     * Add a field to the InputDocument
     *
     * @param field a lucene {@link IndexableField}
     * @return the Builder object
     */
    public Builder addField(IndexableField field) {
      checkFieldName(field.name());
      doc.add(field);
      return this;
    }

    /**
     * Build the InputDocument
     *
     * @return the InputDocument
     */
    public InputDocument build() {
      doc.add(new StringField(ID_FIELD, id, Field.Store.YES));
      PerFieldAnalyzerWrapper analyzerWrapper = new PerFieldAnalyzerWrapper(defaultAnalyzer, analyzers);
      return new InputDocument(id, doc, analyzerWrapper);
    }

  }

  /**
   * Check that a field name does not clash with internal fields required by luwak
   *
   * @param fieldName the field name to check
   * @throws IllegalArgumentException if the field name is reserved
   */
  public static void checkFieldName(String fieldName) {
    if (ID_FIELD.equals(fieldName))
      throw new IllegalArgumentException(ID_FIELD + " is a reserved field name");
  }

}
