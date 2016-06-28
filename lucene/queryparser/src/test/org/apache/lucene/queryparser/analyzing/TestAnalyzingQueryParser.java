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
package org.apache.lucene.queryparser.analyzing;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharacterUtils;
import org.apache.lucene.analysis.MockBytesAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

/**
 */
public class TestAnalyzingQueryParser extends LuceneTestCase {
  private final static String FIELD = "field";
   
  private Analyzer a;

  private String[] wildcardInput;
  private String[] wildcardExpected;
  private String[] prefixInput;
  private String[] prefixExpected;
  private String[] rangeInput;
  private String[] rangeExpected;
  private String[] fuzzyInput;
  private String[] fuzzyExpected;

  private Map<String, String> wildcardEscapeHits = new TreeMap<>();
  private Map<String, String> wildcardEscapeMisses = new TreeMap<>();

  @Override
  public void setUp() throws Exception {
    super.setUp();
    wildcardInput = new String[] { "*bersetzung über*ung",
        "Mötley Cr\u00fce Mötl?* Crü?", "Renée Zellweger Ren?? Zellw?ger" };
    wildcardExpected = new String[] { "*bersetzung uber*ung", "motley crue motl?* cru?",
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

    wildcardEscapeHits.put("mö*tley", "moatley");

    // need to have at least one genuine wildcard to trigger the wildcard analysis
    // hence the * before the y
    wildcardEscapeHits.put("mö\\*tl*y", "mo*tley");

    // escaped backslash then true wildcard
    wildcardEscapeHits.put("mö\\\\*tley", "mo\\atley");
    
    // escaped wildcard then true wildcard
    wildcardEscapeHits.put("mö\\??ley", "mo?tley");

    // the first is an escaped * which should yield a miss
    wildcardEscapeMisses.put("mö\\*tl*y", "moatley");
      
    a = new ASCIIAnalyzer();
  }
   
  public void testWildcardAlone() throws ParseException {
    //seems like crazy edge case, but can be useful in concordance 
    expectThrows(ParseException.class, () -> {
      getAnalyzedQuery("*", a, false);
    });
      
    String qString = parseWithAnalyzingQueryParser("*", a, true);
    assertEquals("Every word", "*", qString);
  }
  public void testWildCardEscapes() throws ParseException, IOException {

    for (Map.Entry<String, String> entry : wildcardEscapeHits.entrySet()){
      Query q = getAnalyzedQuery(entry.getKey(), a, false);
      assertEquals("WildcardEscapeHits: " + entry.getKey(), true, isAHit(q, entry.getValue(), a));
    }
    for (Map.Entry<String, String> entry : wildcardEscapeMisses.entrySet()){
      Query q = getAnalyzedQuery(entry.getKey(), a, false);
      assertEquals("WildcardEscapeMisses: " + entry.getKey(), false, isAHit(q, entry.getValue(), a));
    }

  }
  public void testWildCardQueryNoLeadingAllowed() {
    expectThrows(ParseException.class, () -> {
      parseWithAnalyzingQueryParser(wildcardInput[0], a, false);
    });
  }

  public void testWildCardQuery() throws ParseException {
    for (int i = 0; i < wildcardInput.length; i++) {
      assertEquals("Testing wildcards with analyzer " + a.getClass() + ", input string: "
          + wildcardInput[i], wildcardExpected[i], parseWithAnalyzingQueryParser(wildcardInput[i], a, true));
    }
  }


  public void testPrefixQuery() throws ParseException {
    for (int i = 0; i < prefixInput.length; i++) {
      assertEquals("Testing prefixes with analyzer " + a.getClass() + ", input string: "
          + prefixInput[i], prefixExpected[i], parseWithAnalyzingQueryParser(prefixInput[i], a, false));
    }
  }

  public void testRangeQuery() throws ParseException {
    for (int i = 0; i < rangeInput.length; i++) {
      assertEquals("Testing ranges with analyzer " + a.getClass() + ", input string: "
          + rangeInput[i], rangeExpected[i], parseWithAnalyzingQueryParser(rangeInput[i], a, false));
    }
  }

  public void testFuzzyQuery() throws ParseException {
    for (int i = 0; i < fuzzyInput.length; i++) {
      assertEquals("Testing fuzzys with analyzer " + a.getClass() + ", input string: "
          + fuzzyInput[i], fuzzyExpected[i], parseWithAnalyzingQueryParser(fuzzyInput[i], a, false));
    }
  }


  private String parseWithAnalyzingQueryParser(String s, Analyzer a, boolean allowLeadingWildcard) throws ParseException {
    Query q = getAnalyzedQuery(s, a, allowLeadingWildcard);
    return q.toString(FIELD);
  }

  private Query getAnalyzedQuery(String s, Analyzer a, boolean allowLeadingWildcard) throws ParseException {
    AnalyzingQueryParser qp = new AnalyzingQueryParser(FIELD, a);
    qp.setAllowLeadingWildcard(allowLeadingWildcard);
    org.apache.lucene.search.Query q = qp.parse(s);
    return q;
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

  final static class LowercaseFilter extends TokenFilter {

    final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

    LowercaseFilter(TokenStream input) {
      super(input);
    }

    @Override
    public final boolean incrementToken() throws IOException {
      if (input.incrementToken()) {
        CharacterUtils.toLowerCase(termAtt.buffer(), 0, termAtt.length());
        return true;
      } else
        return false;
    }

  }

  final static class ASCIIAnalyzer extends Analyzer {
    @Override
    public TokenStreamComponents createComponents(String fieldName) {
      Tokenizer result = new MockTokenizer(MockTokenizer.WHITESPACE, true);
      return new TokenStreamComponents(result, new FoldingFilter(result));
    }

    @Override
    protected TokenStream normalize(String fieldName, TokenStream in) {
      return new FoldingFilter(new LowercaseFilter(in));
    }
  }
   

  // LUCENE-4176
  public void testByteTerms() throws Exception {
    String s = "เข";
    Analyzer analyzer = new MockBytesAnalyzer();
    QueryParser qp = new AnalyzingQueryParser(FIELD, analyzer);
    Query q = qp.parse("[เข TO เข]");
    assertEquals(true, isAHit(q, s, analyzer));
  }
   
  
  private boolean isAHit(Query q, String content, Analyzer analyzer) throws IOException{
    Directory ramDir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), ramDir, analyzer);
    Document doc = new Document();
    FieldType fieldType = new FieldType();
    fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    fieldType.setTokenized(true);
    fieldType.setStored(true);
    Field field = new Field(FIELD, content, fieldType);
    doc.add(field);
    writer.addDocument(doc);
    writer.close();
    DirectoryReader ir = DirectoryReader.open(ramDir);
    IndexSearcher is = new IndexSearcher(ir);
      
    int hits = is.search(q, 10).totalHits;
    ir.close();
    ramDir.close();
    if (hits == 1){
      return true;
    } else {
      return false;
    }

  }
}
