package org.apache.lucene.queryparser.analyzing;

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

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;

/**
 */
@SuppressCodecs("Lucene3x") // binary terms
public class TestAnalyzingQueryParser extends LuceneTestCase {

  private Analyzer a;

  private String[] wildcardInput;
  private String[] wildcardExpected;
  private String[] prefixInput;
  private String[] prefixExpected;
  private String[] rangeInput;
  private String[] rangeExpected;
  private String[] fuzzyInput;
  private String[] fuzzyExpected;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    wildcardInput = new String[] { "übersetzung über*ung",
        "Mötley Cr\u00fce Mötl?* Crü?", "Renée Zellweger Ren?? Zellw?ger" };
    wildcardExpected = new String[] { "ubersetzung uber*ung", "motley crue motl?* cru?",
        "renee zellweger ren?? zellw?ger" };

    prefixInput = new String[] { "übersetzung übersetz*",
        "Mötley Crüe Mötl* crü*", "René? Zellw*" };
    prefixExpected = new String[] { "ubersetzung ubersetz*", "motley crue motl* cru*",
        "rene? zellw*" };

    rangeInput = new String[] { "[aa TO bb]", "{Anaïs TO Zoé}" };
    rangeExpected = new String[] { "[aa TO bb]", "{anais TO zoe}" };

    fuzzyInput = new String[] { "Übersetzung Übersetzung~0.9",
        "Mötley Crüe Mötley~0.75 Crüe~0.5",
        "Renée Zellweger Renée~0.9 Zellweger~" };
    fuzzyExpected = new String[] { "ubersetzung ubersetzung~1",
        "motley crue motley~1 crue~2", "renee zellweger renee~0 zellweger~2" };

    a = new ASCIIAnalyzer();
  }

  public void testWildCardQuery() throws ParseException {
    for (int i = 0; i < wildcardInput.length; i++) {
      assertEquals("Testing wildcards with analyzer " + a.getClass() + ", input string: "
          + wildcardInput[i], wildcardExpected[i], parseWithAnalyzingQueryParser(wildcardInput[i], a));
    }
  }

  public void testPrefixQuery() throws ParseException {
    for (int i = 0; i < prefixInput.length; i++) {
      assertEquals("Testing prefixes with analyzer " + a.getClass() + ", input string: "
          + prefixInput[i], prefixExpected[i], parseWithAnalyzingQueryParser(prefixInput[i], a));
    }
  }

  public void testRangeQuery() throws ParseException {
    for (int i = 0; i < rangeInput.length; i++) {
      assertEquals("Testing ranges with analyzer " + a.getClass() + ", input string: "
          + rangeInput[i], rangeExpected[i], parseWithAnalyzingQueryParser(rangeInput[i], a));
    }
  }

  public void testFuzzyQuery() throws ParseException {
    for (int i = 0; i < fuzzyInput.length; i++) {
      assertEquals("Testing fuzzys with analyzer " + a.getClass() + ", input string: "
          + fuzzyInput[i], fuzzyExpected[i], parseWithAnalyzingQueryParser(fuzzyInput[i], a));
    }
  }

  private String parseWithAnalyzingQueryParser(String s, Analyzer a) throws ParseException {
    AnalyzingQueryParser qp = new AnalyzingQueryParser(TEST_VERSION_CURRENT, "field", a);
    org.apache.lucene.search.Query q = qp.parse(s);
    return q.toString("field");
  }
  
  final static class FoldingFilter extends TokenFilter {
    final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

    public FoldingFilter(TokenStream input) {
      super(input);
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (input.incrementToken()) {
        char term[] = termAtt.buffer();
        for (int i = 0; i < term.length; i++)
          switch(term[i]) {
            case 'ü':
              term[i] = 'u'; 
              break;
            case 'ö': 
              term[i] = 'o'; 
              break;
            case 'é': 
              term[i] = 'e'; 
              break;
            case 'ï': 
              term[i] = 'i'; 
              break;
          }
        return true;
      } else {
        return false;
      }
    }
  }

  final static class ASCIIAnalyzer extends Analyzer {
    @Override
    public TokenStreamComponents createComponents(String fieldName, Reader reader) {
      Tokenizer result = new MockTokenizer(reader, MockTokenizer.SIMPLE, true);
      return new TokenStreamComponents(result, new FoldingFilter(result));
    }
  }
  
  // LUCENE-4176
  public void testByteTerms() throws Exception {
    Directory ramDir = newDirectory();
    Analyzer analyzer = new MockBytesAnalyzer();
    RandomIndexWriter writer = new RandomIndexWriter(random(), ramDir, analyzer);
    Document doc = new Document();
    FieldType fieldType = new FieldType();
    fieldType.setIndexed(true);
    fieldType.setTokenized(true);
    fieldType.setStored(true);
    Field field = new Field("content","เข", fieldType);
    doc.add(field);
    writer.addDocument(doc);
    writer.close();
    DirectoryReader ir = DirectoryReader.open(ramDir);
    IndexSearcher is = newSearcher(ir);
    QueryParser qp = new AnalyzingQueryParser(TEST_VERSION_CURRENT, "content", analyzer);
    Query q = qp.parse("[เข TO เข]");
    assertEquals(1, is.search(q, 10).totalHits);
    ir.close();
    ramDir.close();
  }
}