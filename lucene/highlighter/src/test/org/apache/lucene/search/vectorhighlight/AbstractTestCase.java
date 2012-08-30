package org.apache.lucene.search.vectorhighlight;

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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

public abstract class AbstractTestCase extends LuceneTestCase {

  protected final String F = "f";
  protected final String F1 = "f1";
  protected final String F2 = "f2";
  protected Directory dir;
  protected Analyzer analyzerW;
  protected Analyzer analyzerB;
  protected Analyzer analyzerK;
  protected IndexReader reader;  

  protected static final String[] shortMVValues = {
    "",
    "",
    "a b c",
    "",   // empty data in multi valued field
    "d e"
  };
  
  protected static final String[] longMVValues = {
    "Followings are the examples of customizable parameters and actual examples of customization:",
    "The most search engines use only one of these methods. Even the search engines that says they can use the both methods basically"
  };
  
  // test data for LUCENE-1448 bug
  protected static final String[] biMVValues = {
    "\nLucene/Solr does not require such additional hardware.",
    "\nWhen you talk about processing speed, the"
  };
  
  protected static final String[] strMVValues = {
    "abc",
    "defg",
    "hijkl"
  };

  @Override
  public void setUp() throws Exception {
    super.setUp();
    analyzerW = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    analyzerB = new BigramAnalyzer();
    analyzerK = new MockAnalyzer(random(), MockTokenizer.KEYWORD, false);
    dir = newDirectory();
  }
  
  @Override
  public void tearDown() throws Exception {
    if( reader != null ){
      reader.close();
      reader = null;
    }
    dir.close();
    super.tearDown();
  }

  protected Query tq( String text ){
    return tq( 1F, text );
  }

  protected Query tq( float boost, String text ){
    return tq( boost, F, text );
  }
  
  protected Query tq( String field, String text ){
    return tq( 1F, field, text );
  }
  
  protected Query tq( float boost, String field, String text ){
    Query query = new TermQuery( new Term( field, text ) );
    query.setBoost( boost );
    return query;
  }
  
  protected Query pqF( String... texts ){
    return pqF( 1F, texts );
  }
  
  protected Query pqF( float boost, String... texts ){
    return pqF( boost, 0, texts );
  }
  
  protected Query pqF( float boost, int slop, String... texts ){
    return pq( boost, slop, F, texts );
  }
  
  protected Query pq( String field, String... texts ){
    return pq( 1F, 0, field, texts );
  }
  
  protected Query pq( float boost, String field, String... texts ){
    return pq( boost, 0, field, texts );
  }
  
  protected Query pq( float boost, int slop, String field, String... texts ){
    PhraseQuery query = new PhraseQuery();
    for( String text : texts ){
      query.add( new Term( field, text ) );
    }
    query.setBoost( boost );
    query.setSlop( slop );
    return query;
  }
  
  protected Query dmq( Query... queries ){
    return dmq( 0.0F, queries );
  }
  
  protected Query dmq( float tieBreakerMultiplier, Query... queries ){
    DisjunctionMaxQuery query = new DisjunctionMaxQuery( tieBreakerMultiplier );
    for( Query q : queries ){
      query.add( q );
    }
    return query;
  }
  
  protected void assertCollectionQueries( Collection<Query> actual, Query... expected ){
    assertEquals( expected.length, actual.size() );
    for( Query query : expected ){
      assertTrue( actual.contains( query ) );
    }
  }

  protected List<BytesRef> analyze(String text, String field, Analyzer analyzer) throws IOException {
    List<BytesRef> bytesRefs = new ArrayList<BytesRef>();

    TokenStream tokenStream = analyzer.tokenStream(field, new StringReader(text));
    TermToBytesRefAttribute termAttribute = tokenStream.getAttribute(TermToBytesRefAttribute.class);

    BytesRef bytesRef = termAttribute.getBytesRef();

    tokenStream.reset();
    
    while (tokenStream.incrementToken()) {
      termAttribute.fillBytesRef();
      bytesRefs.add(BytesRef.deepCopyOf(bytesRef));
    }

    tokenStream.end();
    tokenStream.close();

    return bytesRefs;
  }

  protected PhraseQuery toPhraseQuery(List<BytesRef> bytesRefs, String field) {
    PhraseQuery phraseQuery = new PhraseQuery();
    for (BytesRef bytesRef : bytesRefs) {
      phraseQuery.add(new Term(field, bytesRef));
    }
    return phraseQuery;
  }

  static final class BigramAnalyzer extends Analyzer {
    @Override
    public TokenStreamComponents createComponents(String fieldName, Reader reader) {
      return new TokenStreamComponents(new BasicNGramTokenizer(reader));
    }
  }
  
  static final class BasicNGramTokenizer extends Tokenizer {

    public static final int DEFAULT_N_SIZE = 2;
    public static final String DEFAULT_DELIMITERS = " \t\n.,";
    private final int n;
    private final String delimiters;
    private int startTerm;
    private int lenTerm;
    private int startOffset;
    private int nextStartOffset;
    private int ch;
    private String snippet;
    private StringBuilder snippetBuffer;
    private static final int BUFFER_SIZE = 4096;
    private char[] charBuffer;
    private int charBufferIndex;
    private int charBufferLen;
    
    public BasicNGramTokenizer( Reader in ){
      this( in, DEFAULT_N_SIZE );
    }
    
    public BasicNGramTokenizer( Reader in, int n ){
      this( in, n, DEFAULT_DELIMITERS );
    }
    
    public BasicNGramTokenizer( Reader in, String delimiters ){
      this( in, DEFAULT_N_SIZE, delimiters );
    }
    
    public BasicNGramTokenizer( Reader in, int n, String delimiters ){
      super(in);
      this.n = n;
      this.delimiters = delimiters;
      startTerm = 0;
      nextStartOffset = 0;
      snippet = null;
      snippetBuffer = new StringBuilder();
      charBuffer = new char[BUFFER_SIZE];
      charBufferIndex = BUFFER_SIZE;
      charBufferLen = 0;
      ch = 0;
    }

    CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
    @Override
    public boolean incrementToken() throws IOException {
      if( !getNextPartialSnippet() )
        return false;
      clearAttributes();
      termAtt.setEmpty().append(snippet, startTerm, startTerm + lenTerm);
      offsetAtt.setOffset(correctOffset(startOffset), correctOffset(startOffset + lenTerm));
      return true;
    }

    private int getFinalOffset() {
      return nextStartOffset;
    }
    
    @Override
    public final void end(){
      offsetAtt.setOffset(getFinalOffset(),getFinalOffset());
    }
    
    protected boolean getNextPartialSnippet() throws IOException {
      if( snippet != null && snippet.length() >= startTerm + 1 + n ){
        startTerm++;
        startOffset++;
        lenTerm = n;
        return true;
      }
      return getNextSnippet();
    }
    
    protected boolean getNextSnippet() throws IOException {
      startTerm = 0;
      startOffset = nextStartOffset;
      snippetBuffer.delete( 0, snippetBuffer.length() );
      while( true ){
        if( ch != -1 )
          ch = readCharFromBuffer();
        if( ch == -1 ) break;
        else if( !isDelimiter( ch ) )
          snippetBuffer.append( (char)ch );
        else if( snippetBuffer.length() > 0 )
          break;
        else
          startOffset++;
      }
      if( snippetBuffer.length() == 0 )
        return false;
      snippet = snippetBuffer.toString();
      lenTerm = snippet.length() >= n ? n : snippet.length();
      return true;
    }
    
    protected int readCharFromBuffer() throws IOException {
      if( charBufferIndex >= charBufferLen ){
        charBufferLen = input.read( charBuffer );
        if( charBufferLen == -1 ){
          return -1;
        }
        charBufferIndex = 0;
      }
      int c = charBuffer[charBufferIndex++];
      nextStartOffset++;
      return c;
    }
    
    protected boolean isDelimiter( int c ){
      return delimiters.indexOf( c ) >= 0;
    }
    
    @Override
    public void reset() {
      startTerm = 0;
      nextStartOffset = 0;
      snippet = null;
      snippetBuffer.setLength( 0 );
      charBufferIndex = BUFFER_SIZE;
      charBufferLen = 0;
      ch = 0;
    }
  }

  protected void make1d1fIndex( String value ) throws Exception {
    make1dmfIndex( value );
  }
  
  protected void make1d1fIndexB( String value ) throws Exception {
    make1dmfIndexB( value );
  }
  
  protected void make1dmfIndex( String... values ) throws Exception {
    make1dmfIndex( analyzerW, values );
  }
  
  protected void make1dmfIndexB( String... values ) throws Exception {
    make1dmfIndex( analyzerB, values );
  }
  
  // make 1 doc with multi valued field
  protected void make1dmfIndex( Analyzer analyzer, String... values ) throws Exception {
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(
        TEST_VERSION_CURRENT, analyzer).setOpenMode(OpenMode.CREATE));
    Document doc = new Document();
    FieldType customType = new FieldType(TextField.TYPE_STORED);
    customType.setStoreTermVectors(true);
    customType.setStoreTermVectorOffsets(true);
    customType.setStoreTermVectorPositions(true);
    for( String value: values ) {
      doc.add( new Field( F, value, customType) );
    }
    writer.addDocument( doc );
    writer.close();
    if (reader != null) reader.close();
    reader = DirectoryReader.open(dir);
  }
  
  // make 1 doc with multi valued & not analyzed field
  protected void make1dmfIndexNA( String... values ) throws Exception {
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(
        TEST_VERSION_CURRENT, analyzerK).setOpenMode(OpenMode.CREATE));
    Document doc = new Document();
    FieldType customType = new FieldType(TextField.TYPE_STORED);
    customType.setStoreTermVectors(true);
    customType.setStoreTermVectorOffsets(true);
    customType.setStoreTermVectorPositions(true);
    for( String value: values ) {
      doc.add( new Field( F, value, customType));
      //doc.add( new Field( F, value, Store.YES, Index.NOT_ANALYZED, TermVector.WITH_POSITIONS_OFFSETS ) );
    }
    writer.addDocument( doc );
    writer.close();
    if (reader != null) reader.close();
    reader = DirectoryReader.open(dir);
  }
  
  protected void makeIndexShortMV() throws Exception {
    
    //  0
    // ""
    //  1
    // ""

    //  234567
    // "a b c"
    //  0 1 2

    //  8
    // ""

    //   111
    //  9012
    // "d e"
    //  3 4
    make1dmfIndex( shortMVValues );
  }
  
  protected void makeIndexLongMV() throws Exception {
    //           11111111112222222222333333333344444444445555555555666666666677777777778888888888999
    // 012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012
    // Followings are the examples of customizable parameters and actual examples of customization:
    // 0          1   2   3        4  5            6          7   8      9        10 11
    
    //        1                                                                                                   2
    // 999999900000000001111111111222222222233333333334444444444555555555566666666667777777777888888888899999999990000000000111111111122
    // 345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901
    // The most search engines use only one of these methods. Even the search engines that says they can use the both methods basically
    // 12  13  (14)   (15)     16  17   18  19 20    21       22   23 (24)   (25)     26   27   28   29  30  31  32   33      34

    make1dmfIndex( longMVValues );
  }
  
  protected void makeIndexLongMVB() throws Exception {
    // "*" ... LF
    
    //           1111111111222222222233333333334444444444555555
    // 01234567890123456789012345678901234567890123456789012345
    // *Lucene/Solr does not require such additional hardware.
    //  Lu 0        do 10    re 15   su 21       na 31
    //   uc 1        oe 11    eq 16   uc 22       al 32
    //    ce 2        es 12    qu 17   ch 23         ha 33
    //     en 3          no 13  ui 18     ad 24       ar 34
    //      ne 4          ot 14  ir 19     dd 25       rd 35
    //       e/ 5                 re 20     di 26       dw 36
    //        /S 6                           it 27       wa 37
    //         So 7                           ti 28       ar 38
    //          ol 8                           io 29       re 39
    //           lr 9                           on 30

    // 5555666666666677777777778888888888999999999
    // 6789012345678901234567890123456789012345678
    // *When you talk about processing speed, the
    //  Wh 40         ab 48     es 56         th 65
    //   he 41         bo 49     ss 57         he 66
    //    en 42         ou 50     si 58
    //       yo 43       ut 51     in 59
    //        ou 44         pr 52   ng 60
    //           ta 45       ro 53     sp 61
    //            al 46       oc 54     pe 62
    //             lk 47       ce 55     ee 63
    //                                    ed 64

    make1dmfIndexB( biMVValues );
  }
  
  protected void makeIndexStrMV() throws Exception {

    //  0123
    // "abc"
    
    //  34567
    // "defg"

    //     111
    //  789012
    // "hijkl"
    make1dmfIndexNA( strMVValues );
  }
}
