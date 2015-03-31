package org.apache.lucene.search.suggest.document;

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

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.BytesRef;

/**
 * <p>
 * Field that indexes a string value and a weight as a weighted completion
 * against a named suggester.
 * Field is tokenized, not stored and stores documents, frequencies and positions.
 * Field can be used to provide near real time document suggestions.
 * </p>
 * <p>
 * Besides the usual {@link org.apache.lucene.analysis.Analyzer}s,
 * {@link CompletionAnalyzer}
 * can be used to tune suggest field only parameters
 * (e.g. preserving token seperators, preserving position increments
 * when converting the token stream to an automaton)
 * </p>
 * <p>
 * Example indexing usage:
 * <pre class="prettyprint">
 * document.add(new SuggestField(name, "suggestion", 4));
 * </pre>
 * To perform document suggestions based on the this field, use
 * {@link SuggestIndexSearcher#suggest(String, CharSequence, int, org.apache.lucene.search.Filter)}
 * <p>
 * Example query usage:
 * <pre class="prettyprint">
 * SuggestIndexSearcher indexSearcher = ..
 * indexSearcher.suggest(name, "su", 2)
 * </pre>
 *
 * @lucene.experimental
 */
public class SuggestField extends Field {

  private static final FieldType FIELD_TYPE = new FieldType();

  static {
    FIELD_TYPE.setTokenized(true);
    FIELD_TYPE.setStored(false);
    FIELD_TYPE.setStoreTermVectors(false);
    FIELD_TYPE.setOmitNorms(false);
    FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    FIELD_TYPE.freeze();
  }

  private final BytesRef surfaceForm;
  private final long weight;

  /**
   * Creates a {@link SuggestField}
   *
   * @param name   of the field
   * @param value  to get suggestions on
   * @param weight weight of the suggestion
   */
  public SuggestField(String name, String value, long weight) {
    super(name, value, FIELD_TYPE);
    if (weight < 0l) {
      throw new IllegalArgumentException("weight must be >= 0");
    }
    this.surfaceForm = new BytesRef(value);
    this.weight = weight;
  }

  @Override
  public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) throws IOException {
    TokenStream stream = super.tokenStream(analyzer, reuse);
    CompletionTokenStream completionStream;
    if (stream instanceof CompletionTokenStream) {
      completionStream = (CompletionTokenStream) stream;
    } else {
      completionStream = new CompletionTokenStream(stream);
    }
    BytesRef suggestPayload = buildSuggestPayload(surfaceForm, weight, (char) completionStream.sepLabel());
    completionStream.setPayload(suggestPayload);
    return completionStream;
  }

  private BytesRef buildSuggestPayload(BytesRef surfaceForm, long weight, char sepLabel) throws IOException {
    for (int i = 0; i < surfaceForm.length; i++) {
      if (surfaceForm.bytes[i] == sepLabel) {
        assert sepLabel == '\u001f';
        throw new IllegalArgumentException(
            "surface form cannot contain unit separator character U+001F; this character is reserved");
      }
    }
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try (OutputStreamDataOutput output = new OutputStreamDataOutput(byteArrayOutputStream)) {
      output.writeVInt(surfaceForm.length);
      output.writeBytes(surfaceForm.bytes, surfaceForm.offset, surfaceForm.length);
      output.writeVLong(weight + 1);
    }
    return new BytesRef(byteArrayOutputStream.toByteArray());
  }
}
