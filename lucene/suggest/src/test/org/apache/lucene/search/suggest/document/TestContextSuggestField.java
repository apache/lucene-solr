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
package org.apache.lucene.search.suggest.document;

import java.io.ByteArrayOutputStream;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.ConcatenateGraphFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.lucene.analysis.BaseTokenStreamTestCase.assertTokenStreamContents;
import static org.apache.lucene.search.suggest.document.TestSuggestField.Entry;
import static org.apache.lucene.search.suggest.document.TestSuggestField.assertSuggestions;
import static org.apache.lucene.search.suggest.document.TestSuggestField.iwcWithSuggestField;

public class TestContextSuggestField extends LuceneTestCase {

  public Directory dir;

  @Before
  public void before() throws Exception {
    dir = newDirectory();
  }

  @After
  public void after() throws Exception {
    dir.close();
  }

  @Test
  public void testEmptySuggestion() throws Exception {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new ContextSuggestField("suggest_field", "", 1, "type1");
    });
    assertTrue(expected.getMessage().contains("value"));
  }

  @Test
  public void testReservedChars() throws Exception {
    CharsRefBuilder charsRefBuilder = new CharsRefBuilder();
    charsRefBuilder.append("sugg");
    charsRefBuilder.setCharAt(2, (char) ContextSuggestField.CONTEXT_SEPARATOR);

    Analyzer analyzer = new MockAnalyzer(random());
    Document document = new Document();
    try (RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "name"))) {
      // exception should be thrown for context value containing CONTEXT_SEPARATOR
      IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
        document.add(new ContextSuggestField("name", "sugg", 1, charsRefBuilder.toString()));
        iw.addDocument(document);
        iw.commit();
      });
      assertTrue(expected.getMessage().contains("[0x1d]"));
    }
    document.clear();

    try (RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "name"))) {
      // exception should be thrown for context value containing CONTEXT_SEPARATOR
      IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
        document.add(new ContextSuggestField("name", charsRefBuilder.toString(), 1, "sugg"));
        iw.addDocument(document);
        iw.commit(false);
      });
      assertTrue(expected.getMessage().contains("[0x1d]"));
    }
  }

  @Test
  public void testTokenStream() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    ContextSuggestField field = new ContextSuggestField("field", "input", 1, "context1", "context2");
    BytesRef surfaceForm = new BytesRef("input");
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try (OutputStreamDataOutput output = new OutputStreamDataOutput(byteArrayOutputStream)) {
      output.writeVInt(surfaceForm.length);
      output.writeBytes(surfaceForm.bytes, surfaceForm.offset, surfaceForm.length);
      output.writeVInt(1 + 1);
      output.writeByte(ContextSuggestField.TYPE);
    }
    BytesRef payload = new BytesRef(byteArrayOutputStream.toByteArray());
    String[] expectedOutputs = new String[2];
    CharsRefBuilder builder = new CharsRefBuilder();
    builder.append("context1");
    builder.append(((char) ContextSuggestField.CONTEXT_SEPARATOR));
    builder.append((char) ConcatenateGraphFilter.SEP_LABEL);
    builder.append("input");
    expectedOutputs[0] = builder.toCharsRef().toString();
    builder.clear();
    builder.append("context2");
    builder.append(((char) ContextSuggestField.CONTEXT_SEPARATOR));
    builder.append((char) ConcatenateGraphFilter.SEP_LABEL);
    builder.append("input");
    expectedOutputs[1] = builder.toCharsRef().toString();
    TokenStream stream = new TestSuggestField.PayloadAttrToTypeAttrFilter(field.tokenStream(analyzer, null));
    assertTokenStreamContents(stream, expectedOutputs, null, null, new String[]{payload.utf8ToString(), payload.utf8ToString()}, new int[]{1, 0}, null, null);

    CompletionAnalyzer completionAnalyzer = new CompletionAnalyzer(analyzer);
    stream = new TestSuggestField.PayloadAttrToTypeAttrFilter(field.tokenStream(completionAnalyzer, null));
    assertTokenStreamContents(stream, expectedOutputs, null, null, new String[]{payload.utf8ToString(), payload.utf8ToString()}, new int[]{1, 0}, null, null);
  }

  @Test
  public void testMixedSuggestFields() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    Document document = new Document();
    document.add(new SuggestField("suggest_field", "suggestion1", 4));
    document.add(new ContextSuggestField("suggest_field", "suggestion2", 3));

    try (RandomIndexWriter iw = new RandomIndexWriter(random(), dir,
        iwcWithSuggestField(analyzer, "suggest_field"))) {
      // mixing suggest field types for same field name should error out
      IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
        iw.addDocument(document);
        iw.commit(false);
      });
      assertTrue(expected.getMessage().contains("mixed types"));
    }
  }

  @Test
  public void testWithSuggestFields() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir,
        iwcWithSuggestField(analyzer, "suggest_field", "context_suggest_field"));
    Document document = new Document();

    document.add(new SuggestField("suggest_field", "suggestion1", 4));
    document.add(new SuggestField("suggest_field", "suggestion2", 3));
    document.add(new SuggestField("suggest_field", "suggestion3", 2));
    document.add(new ContextSuggestField("context_suggest_field", "suggestion1", 4, "type1"));
    document.add(new ContextSuggestField("context_suggest_field", "suggestion2", 3, "type2"));
    document.add(new ContextSuggestField("context_suggest_field", "suggestion3", 2, "type3"));
    iw.addDocument(document);

    document = new Document();
    document.add(new SuggestField("suggest_field", "suggestion4", 1));
    document.add(new ContextSuggestField("context_suggest_field", "suggestion4", 1, "type4"));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);

    CompletionQuery query = new PrefixCompletionQuery(analyzer, new Term("suggest_field", "sugg"));
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(query, 10, false);
    assertSuggestions(suggest,
        new Entry("suggestion1", 4),
        new Entry("suggestion2", 3),
        new Entry("suggestion3", 2),
        new Entry("suggestion4", 1));

    query = new PrefixCompletionQuery(analyzer, new Term("context_suggest_field", "sugg"));
    suggest = suggestIndexSearcher.suggest(query, 10, false);
    assertSuggestions(suggest,
        new Entry("suggestion1", "type1", 4),
        new Entry("suggestion2", "type2", 3),
        new Entry("suggestion3", "type3", 2),
        new Entry("suggestion4", "type4", 1));

    reader.close();
    iw.close();
  }

  @Test
  public void testCompletionAnalyzer() throws Exception {
    CompletionAnalyzer completionAnalyzer = new CompletionAnalyzer(new StandardAnalyzer(), true, true);
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(completionAnalyzer, "suggest_field"));
    Document document = new Document();

    document.add(new ContextSuggestField("suggest_field", "suggestion1", 4, "type1"));
    document.add(new ContextSuggestField("suggest_field", "suggestion2", 3, "type2"));
    document.add(new ContextSuggestField("suggest_field", "suggestion3", 2, "type3"));
    iw.addDocument(document);
    document = new Document();
    document.add(new ContextSuggestField("suggest_field", "suggestion4", 1, "type4"));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    ContextQuery query = new ContextQuery(new PrefixCompletionQuery(completionAnalyzer, new Term("suggest_field", "sugg")));
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(query, 4, false);
    assertSuggestions(suggest,
        new Entry("suggestion1", "type1", 4),
        new Entry("suggestion2", "type2", 3),
        new Entry("suggestion3", "type3", 2),
        new Entry("suggestion4", "type4", 1));
    query.addContext("type1");
    suggest = suggestIndexSearcher.suggest(query, 4, false);
    assertSuggestions(suggest,
        new Entry("suggestion1", "type1", 4));
    reader.close();
    iw.close();
  }
}
