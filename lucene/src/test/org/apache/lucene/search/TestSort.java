package org.apache.lucene.search;

/**
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
import java.io.Serializable;
import java.text.Collator;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.FieldValueHitQueue.Entry;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.DocIdBitSet;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.junit.BeforeClass;

/**
 * Unit tests for sorting code.
 *
 * <p>Created: Feb 17, 2004 4:55:10 PM
 *
 * @since   lucene 1.4
 */

public class TestSort extends LuceneTestCase implements Serializable {

  private static int NUM_STRINGS;
  private IndexSearcher full;
  private IndexSearcher searchX;
  private IndexSearcher searchY;
  private Query queryX;
  private Query queryY;
  private Query queryA;
  private Query queryE;
  private Query queryF;
  private Query queryG;
  private Query queryM;
  private Sort sort;

  @BeforeClass
  public static void beforeClass() throws Exception {
    NUM_STRINGS = atLeast(6000);
  }
  // document data:
  // the tracer field is used to determine which document was hit
  // the contents field is used to search and sort by relevance
  // the int field to sort by int
  // the float field to sort by float
  // the string field to sort by string
    // the i18n field includes accented characters for testing locale-specific sorting
  private String[][] data = new String[][] {
  // tracer  contents         int            float           string   custom   i18n               long            double,          short,     byte, 'custom parser encoding'
  {   "A",   "x a",           "5",           "4f",           "c",     "A-3",   "p\u00EAche",      "10",           "-4.0",            "3",    "126", "J"},//A, x
  {   "B",   "y a",           "5",           "3.4028235E38", "i",     "B-10",  "HAT",             "1000000000",   "40.0",           "24",      "1", "I"},//B, y
  {   "C",   "x a b c",       "2147483647",  "1.0",          "j",     "A-2",   "p\u00E9ch\u00E9", "99999999","40.00002343",        "125",     "15", "H"},//C, x
  {   "D",   "y a b c",       "-1",          "0.0f",         "a",     "C-0",   "HUT",   String.valueOf(Long.MAX_VALUE),String.valueOf(Double.MIN_VALUE), String.valueOf(Short.MIN_VALUE), String.valueOf(Byte.MIN_VALUE), "G"},//D, y
  {   "E",   "x a b c d",     "5",           "2f",           "h",     "B-8",   "peach", String.valueOf(Long.MIN_VALUE),String.valueOf(Double.MAX_VALUE), String.valueOf(Short.MAX_VALUE),           String.valueOf(Byte.MAX_VALUE), "F"},//E,x
  {   "F",   "y a b c d",     "2",           "3.14159f",     "g",     "B-1",   "H\u00C5T",        "-44",          "343.034435444",  "-3",      "0", "E"},//F,y
  {   "G",   "x a b c d",     "3",           "-1.0",         "f",     "C-100", "sin",             "323254543543", "4.043544",        "5",    "100", "D"},//G,x
  {   "H",   "y a b c d",     "0",           "1.4E-45",      "e",     "C-88",  "H\u00D8T",        "1023423423005","4.043545",       "10",    "-50", "C"},//H,y
  {   "I",   "x a b c d e f", "-2147483648", "1.0e+0",       "d",     "A-10",  "s\u00EDn",        "332422459999", "4.043546",     "-340",     "51", "B"},//I,x
  {   "J",   "y a b c d e f", "4",           ".5",           "b",     "C-7",   "HOT",             "34334543543",  "4.0000220343",  "300",      "2", "A"},//J,y
  {   "W",   "g",             "1",           null,           null,    null,    null,              null,           null, null, null, null},
  {   "X",   "g",             "1",           "0.1",          null,    null,    null,              null,           null, null, null, null},
  {   "Y",   "g",             "1",           "0.2",          null,    null,    null,              null,           null, null, null, null},
  {   "Z",   "f g",           null,          null,           null,    null,    null,              null,           null, null, null, null},
  
  // Sort Missing first/last
  {   "a",   "m",            null,          null,           null,    null,    null,              null,           null, null, null, null},
  {   "b",   "m",            "4",           "4.0",           "4",    null,    null,              "4",           "4", "4", "4", null},
  {   "c",   "m",            "5",           "5.0",           "5",    null,    null,              "5",           "5", "5", "5", null},
  {   "d",   "m",            null,          null,           null,    null,    null,              null,           null, null, null, null}
  }; 
  
  // the sort order of Ø versus U depends on the version of the rules being used
  // for the inherited root locale: Ø's order isnt specified in Locale.US since 
  // its not used in english.
  private boolean oStrokeFirst = Collator.getInstance(new Locale("")).compare("Ø", "U") < 0;
  
  // create an index of all the documents, or just the x, or just the y documents
  private IndexSearcher getIndex (boolean even, boolean odd)
  throws IOException {
    Directory indexStore = newDirectory();
    dirs.add(indexStore);
    RandomIndexWriter writer = new RandomIndexWriter(random, indexStore, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMergePolicy(newLogMergePolicy()));

    for (int i=0; i<data.length; ++i) {
      if (((i%2)==0 && even) || ((i%2)==1 && odd)) {
        Document doc = new Document();
        doc.add (new Field ("tracer",   data[i][0], Field.Store.YES, Field.Index.NO));
        doc.add (new Field ("contents", data[i][1], Field.Store.NO, Field.Index.ANALYZED));
        if (data[i][2] != null) doc.add (new Field ("int",      data[i][2], Field.Store.NO, Field.Index.NOT_ANALYZED));
        if (data[i][3] != null) doc.add (new Field ("float",    data[i][3], Field.Store.NO, Field.Index.NOT_ANALYZED));
        if (data[i][4] != null) doc.add (new Field ("string",   data[i][4], Field.Store.NO, Field.Index.NOT_ANALYZED));
        if (data[i][5] != null) doc.add (new Field ("custom",   data[i][5], Field.Store.NO, Field.Index.NOT_ANALYZED));
        if (data[i][6] != null) doc.add (new Field ("i18n",     data[i][6], Field.Store.NO, Field.Index.NOT_ANALYZED));
        if (data[i][7] != null) doc.add (new Field ("long",     data[i][7], Field.Store.NO, Field.Index.NOT_ANALYZED));
        if (data[i][8] != null) doc.add (new Field ("double",     data[i][8], Field.Store.NO, Field.Index.NOT_ANALYZED));
        if (data[i][9] != null) doc.add (new Field ("short",     data[i][9], Field.Store.NO, Field.Index.NOT_ANALYZED));
        if (data[i][10] != null) doc.add (new Field ("byte",     data[i][10], Field.Store.NO, Field.Index.NOT_ANALYZED));
        if (data[i][11] != null) doc.add (new Field ("parser",     data[i][11], Field.Store.NO, Field.Index.NOT_ANALYZED));
        doc.setBoost(2);  // produce some scores above 1.0
        writer.addDocument (doc);
      }
    }
    IndexReader reader = writer.getReader();
    writer.close ();
    IndexSearcher s = newSearcher(reader);
    s.setDefaultFieldSortScoring(true, true);
    return s;
  }

  private IndexSearcher getFullIndex()
  throws IOException {
    return getIndex (true, true);
  }
  
  private IndexSearcher getFullStrings() throws CorruptIndexException, LockObtainFailedException, IOException {
    Directory indexStore = newDirectory();
    dirs.add(indexStore);
    IndexWriter writer = new IndexWriter(
        indexStore,
        new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).
            setMaxBufferedDocs(4).
            setMergePolicy(newLogMergePolicy(97))
    );
    for (int i=0; i<NUM_STRINGS; i++) {
        Document doc = new Document();
        String num = getRandomCharString(getRandomNumber(2, 8), 48, 52);
        doc.add (new Field ("tracer", num, Field.Store.YES, Field.Index.NO));
        //doc.add (new Field ("contents", Integer.toString(i), Field.Store.NO, Field.Index.ANALYZED));
        doc.add (new Field ("string", num, Field.Store.NO, Field.Index.NOT_ANALYZED));
        String num2 = getRandomCharString(getRandomNumber(1, 4), 48, 50);
        doc.add (new Field ("string2", num2, Field.Store.NO, Field.Index.NOT_ANALYZED));
        doc.add (new Field ("tracer2", num2, Field.Store.YES, Field.Index.NO));
        doc.setBoost(2);  // produce some scores above 1.0
        writer.addDocument (doc);
      
    }
    //writer.forceMerge(1);
    //System.out.println(writer.getSegmentCount());
    writer.close();
    IndexReader reader = IndexReader.open(indexStore);
    return new IndexSearcher (reader);
  }
  
  public String getRandomNumberString(int num, int low, int high) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < num; i++) {
      sb.append(getRandomNumber(low, high));
    }
    return sb.toString();
  }
  
  public String getRandomCharString(int num) {
    return getRandomCharString(num, 48, 122);
  }
  
  public String getRandomCharString(int num, int start, int end) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < num; i++) {
      sb.append(new Character((char) getRandomNumber(start, end)));
    }
    return sb.toString();
  }
  
  public int getRandomNumber(final int low, final int high) {
  
    int randInt = (Math.abs(random.nextInt()) % (high - low)) + low;

    return randInt;
  }

  private IndexSearcher getXIndex()
  throws IOException {
    return getIndex (true, false);
  }

  private IndexSearcher getYIndex()
  throws IOException {
    return getIndex (false, true);
  }

  private IndexSearcher getEmptyIndex()
  throws IOException {
    return getIndex (false, false);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    full = getFullIndex();
    searchX = getXIndex();
    searchY = getYIndex();
    queryX = new TermQuery (new Term ("contents", "x"));
    queryY = new TermQuery (new Term ("contents", "y"));
    queryA = new TermQuery (new Term ("contents", "a"));
    queryE = new TermQuery (new Term ("contents", "e"));
    queryF = new TermQuery (new Term ("contents", "f"));
    queryG = new TermQuery (new Term ("contents", "g"));
    queryM = new TermQuery (new Term ("contents", "m"));
    sort = new Sort();
  }
  
  private ArrayList<Directory> dirs = new ArrayList<Directory>();
  
  @Override
  public void tearDown() throws Exception {
    full.reader.close();
    searchX.reader.close();
    searchY.reader.close();
    full.close();
    searchX.close();
    searchY.close();
    for (Directory dir : dirs)
      dir.close();
    super.tearDown();
  }

  // test the sorts by score and document number
  public void testBuiltInSorts() throws Exception {
    sort = new Sort();
    assertMatches (full, queryX, sort, "ACEGI");
    assertMatches (full, queryY, sort, "BDFHJ");

    sort.setSort(SortField.FIELD_DOC);
    assertMatches (full, queryX, sort, "ACEGI");
    assertMatches (full, queryY, sort, "BDFHJ");
  }

  // test sorts where the type of field is specified
  public void testTypedSort() throws Exception {
    sort.setSort (new SortField ("int", SortField.INT), SortField.FIELD_DOC );
    assertMatches (full, queryX, sort, "IGAEC");
    assertMatches (full, queryY, sort, "DHFJB");

    sort.setSort (new SortField ("float", SortField.FLOAT), SortField.FIELD_DOC );
    assertMatches (full, queryX, sort, "GCIEA");
    assertMatches (full, queryY, sort, "DHJFB");

    sort.setSort (new SortField ("long", SortField.LONG), SortField.FIELD_DOC );
    assertMatches (full, queryX, sort, "EACGI");
    assertMatches (full, queryY, sort, "FBJHD");

    sort.setSort (new SortField ("double", SortField.DOUBLE), SortField.FIELD_DOC );
    assertMatches (full, queryX, sort, "AGICE");
    assertMatches (full, queryY, sort, "DJHBF");

    sort.setSort (new SortField ("byte", SortField.BYTE), SortField.FIELD_DOC );
    assertMatches (full, queryX, sort, "CIGAE");
    assertMatches (full, queryY, sort, "DHFBJ");

    sort.setSort (new SortField ("short", SortField.SHORT), SortField.FIELD_DOC );
    assertMatches (full, queryX, sort, "IAGCE");
    assertMatches (full, queryY, sort, "DFHBJ");

    sort.setSort (new SortField ("string", SortField.STRING), SortField.FIELD_DOC );
    assertMatches (full, queryX, sort, "AIGEC");
    assertMatches (full, queryY, sort, "DJHFB");
  }
  
  private static class SortMissingLastTestHelper {
    final SortField sortField;
    final Object min;
    final Object max;
    
    SortMissingLastTestHelper( SortField sortField, Object min, Object max ) {
      this.sortField = sortField;
      this.min = min;
      this.max = max;
    }
  }

  // test sorts where the type of field is specified
  public void testSortMissingLast() throws Exception {
    
    @SuppressWarnings("boxing")
    SortMissingLastTestHelper[] ascendTesters = new SortMissingLastTestHelper[] {
        new SortMissingLastTestHelper( new SortField(   "byte",   SortField.BYTE ), Byte.MIN_VALUE,    Byte.MAX_VALUE ),
        new SortMissingLastTestHelper( new SortField(  "short",  SortField.SHORT ), Short.MIN_VALUE,   Short.MAX_VALUE ),
        new SortMissingLastTestHelper( new SortField(    "int",    SortField.INT ), Integer.MIN_VALUE, Integer.MAX_VALUE ),
        new SortMissingLastTestHelper( new SortField(   "long",   SortField.LONG ), Long.MIN_VALUE,    Long.MAX_VALUE ),
        new SortMissingLastTestHelper( new SortField(  "float",  SortField.FLOAT ), Float.MIN_VALUE,   Float.MAX_VALUE ),
        new SortMissingLastTestHelper( new SortField( "double", SortField.DOUBLE ), Double.MIN_VALUE,  Double.MAX_VALUE ),
    };
    
    @SuppressWarnings("boxing")
    SortMissingLastTestHelper[] descendTesters = new SortMissingLastTestHelper[] {
      new SortMissingLastTestHelper( new SortField(   "byte",   SortField.BYTE, true ), Byte.MIN_VALUE,    Byte.MAX_VALUE ),
      new SortMissingLastTestHelper( new SortField(  "short",  SortField.SHORT, true ), Short.MIN_VALUE,   Short.MAX_VALUE ),
      new SortMissingLastTestHelper( new SortField(    "int",    SortField.INT, true ), Integer.MIN_VALUE, Integer.MAX_VALUE ),
      new SortMissingLastTestHelper( new SortField(   "long",   SortField.LONG, true ), Long.MIN_VALUE,    Long.MAX_VALUE ),
      new SortMissingLastTestHelper( new SortField(  "float",  SortField.FLOAT, true ), Float.MIN_VALUE,   Float.MAX_VALUE ),
      new SortMissingLastTestHelper( new SortField( "double", SortField.DOUBLE, true ), Double.MIN_VALUE,  Double.MAX_VALUE ),
    };
    
    // Default order: ascending
    for( SortMissingLastTestHelper t : ascendTesters ) {
      sort.setSort (t.sortField, SortField.FIELD_DOC );
      assertMatches("sortField:"+t.sortField, full, queryM, sort, "adbc" );

      sort.setSort (t.sortField.setMissingValue( t.max ), SortField.FIELD_DOC );
      assertMatches("sortField:"+t.sortField, full, queryM, sort, "bcad" );

      sort.setSort (t.sortField.setMissingValue( t.min ), SortField.FIELD_DOC );
      assertMatches("sortField:"+t.sortField, full, queryM, sort, "adbc" );
    }
    
    // Reverse order: descending (Note: Order for un-valued documents remains the same due to tie breaker: a,d)
    for( SortMissingLastTestHelper t : descendTesters ) {
      sort.setSort (t.sortField, SortField.FIELD_DOC );
      assertMatches("sortField:"+t.sortField, full, queryM, sort, "cbad" );
      
      sort.setSort (t.sortField.setMissingValue( t.max ), SortField.FIELD_DOC );
      assertMatches("sortField:"+t.sortField, full, queryM, sort, "adcb" );
      
      sort.setSort (t.sortField.setMissingValue( t.min ), SortField.FIELD_DOC );
      assertMatches("sortField:"+t.sortField, full, queryM, sort, "cbad" );
    }
    
    
  }
  
  /**
   * Test String sorting: small queue to many matches, multi field sort, reverse sort
   */
  public void testStringSort() throws IOException, ParseException {
    ScoreDoc[] result = null;
    IndexSearcher searcher = getFullStrings();
    sort.setSort(
        new SortField("string", SortField.STRING),
        new SortField("string2", SortField.STRING, true),
        SortField.FIELD_DOC );

    result = searcher.search(new MatchAllDocsQuery(), null, 500, sort).scoreDocs;

    StringBuilder buff = new StringBuilder();
    int n = result.length;
    String last = null;
    String lastSub = null;
    int lastDocId = 0;
    boolean fail = false;
    for (int x = 0; x < n; ++x) {
      Document doc2 = searcher.doc(result[x].doc);
      String[] v = doc2.getValues("tracer");
      String[] v2 = doc2.getValues("tracer2");
      for (int j = 0; j < v.length; ++j) {
        if (last != null) {
          int cmp = v[j].compareTo(last);
          if (!(cmp >= 0)) { // ensure first field is in order
            fail = true;
            System.out.println("fail:" + v[j] + " < " + last);
          }
          if (cmp == 0) { // ensure second field is in reverse order
            cmp = v2[j].compareTo(lastSub);
            if (cmp > 0) {
              fail = true;
              System.out.println("rev field fail:" + v2[j] + " > " + lastSub);
            } else if(cmp == 0) { // ensure docid is in order
              if (result[x].doc < lastDocId) {
                fail = true;
                System.out.println("doc fail:" + result[x].doc + " > " + lastDocId);
              }
            }
          }
        }
        last = v[j];
        lastSub = v2[j];
        lastDocId = result[x].doc;
        buff.append(v[j] + "(" + v2[j] + ")(" + result[x].doc+") ");
      }
    }
    if(fail) {
      System.out.println("topn field1(field2)(docID):" + buff);
    }
    assertFalse("Found sort results out of order", fail);
    searcher.getIndexReader().close();
    searcher.close();
  }
  
  /** 
   * test sorts where the type of field is specified and a custom field parser 
   * is used, that uses a simple char encoding. The sorted string contains a 
   * character beginning from 'A' that is mapped to a numeric value using some 
   * "funny" algorithm to be different for each data type.
   */
  public void testCustomFieldParserSort() throws Exception {
    // since tests explicilty uses different parsers on the same fieldname
    // we explicitly check/purge the FieldCache between each assertMatch
    FieldCache fc = FieldCache.DEFAULT;


    sort.setSort (new SortField ("parser", new FieldCache.IntParser(){
      public final int parseInt(final String val) {
        return (val.charAt(0)-'A') * 123456;
      }
    }), SortField.FIELD_DOC );
    assertMatches (full, queryA, sort, "JIHGFEDCBA");
    assertSaneFieldCaches(getName() + " IntParser");
    fc.purgeAllCaches();

    sort.setSort (new SortField ("parser", new FieldCache.FloatParser(){
      public final float parseFloat(final String val) {
        return (float) Math.sqrt( val.charAt(0) );
      }
    }), SortField.FIELD_DOC );
    assertMatches (full, queryA, sort, "JIHGFEDCBA");
    assertSaneFieldCaches(getName() + " FloatParser");
    fc.purgeAllCaches();

    sort.setSort (new SortField ("parser", new FieldCache.LongParser(){
      public final long parseLong(final String val) {
        return (val.charAt(0)-'A') * 1234567890L;
      }
    }), SortField.FIELD_DOC );
    assertMatches (full, queryA, sort, "JIHGFEDCBA");
    assertSaneFieldCaches(getName() + " LongParser");
    fc.purgeAllCaches();

    sort.setSort (new SortField ("parser", new FieldCache.DoubleParser(){
      public final double parseDouble(final String val) {
        return Math.pow( val.charAt(0), (val.charAt(0)-'A') );
      }
    }), SortField.FIELD_DOC );
    assertMatches (full, queryA, sort, "JIHGFEDCBA");
    assertSaneFieldCaches(getName() + " DoubleParser");
    fc.purgeAllCaches();

    sort.setSort (new SortField ("parser", new FieldCache.ByteParser(){
      public final byte parseByte(final String val) {
        return (byte) (val.charAt(0)-'A');
      }
    }), SortField.FIELD_DOC );
    assertMatches (full, queryA, sort, "JIHGFEDCBA");
    assertSaneFieldCaches(getName() + " ByteParser");
    fc.purgeAllCaches();

    sort.setSort (new SortField ("parser", new FieldCache.ShortParser(){
      public final short parseShort(final String val) {
        return (short) (val.charAt(0)-'A');
      }
    }), SortField.FIELD_DOC );
    assertMatches (full, queryA, sort, "JIHGFEDCBA");
    assertSaneFieldCaches(getName() + " ShortParser");
    fc.purgeAllCaches();
  }

  // test sorts when there's nothing in the index
  public void testEmptyIndex() throws Exception {
    Searcher empty = getEmptyIndex();

    sort = new Sort();
    assertMatches (empty, queryX, sort, "");

    sort.setSort(SortField.FIELD_DOC);
    assertMatches (empty, queryX, sort, "");

    sort.setSort (new SortField ("int", SortField.INT), SortField.FIELD_DOC );
    assertMatches (empty, queryX, sort, "");

    sort.setSort (new SortField ("string", SortField.STRING, true), SortField.FIELD_DOC );
    assertMatches (empty, queryX, sort, "");

    sort.setSort (new SortField ("float", SortField.FLOAT), new SortField ("string", SortField.STRING) );
    assertMatches (empty, queryX, sort, "");
  }

  static class MyFieldComparator extends FieldComparator<Integer> {
    int[] docValues;
    int[] slotValues;
    int bottomValue;

    MyFieldComparator(int numHits) {
      slotValues = new int[numHits];
    }

    @Override
    public void copy(int slot, int doc) {
      slotValues[slot] = docValues[doc];
    }

    @Override
    public int compare(int slot1, int slot2) {
      // values are small enough that overflow won't happen
      return slotValues[slot1] - slotValues[slot2];
    }

    @Override
    public int compareBottom(int doc) {
      return bottomValue - docValues[doc];
    }

    @Override
    public void setBottom(int bottom) {
      bottomValue = slotValues[bottom];
    }

    private static final FieldCache.IntParser testIntParser = new FieldCache.IntParser() {
      public final int parseInt(final String val) {
        return (val.charAt(0)-'A') * 123456;
      }
    };

    @Override
    public void setNextReader(IndexReader reader, int docBase) throws IOException {
      docValues = FieldCache.DEFAULT.getInts(reader, "parser", testIntParser);
    }

    @Override
    public Integer value(int slot) {
      return Integer.valueOf(slotValues[slot]);
    }
  }

  static class MyFieldComparatorSource extends FieldComparatorSource {
    @Override
    public FieldComparator newComparator(String fieldname, int numHits, int sortPos, boolean reversed) {
      return new MyFieldComparator(numHits);
    }
  }

  // Test sorting w/ custom FieldComparator
  public void testNewCustomFieldParserSort() throws Exception {
    sort.setSort (new SortField ("parser", new MyFieldComparatorSource()));
    assertMatches (full, queryA, sort, "JIHGFEDCBA");
  }

  // test sorts in reverse
  public void testReverseSort() throws Exception {
    sort.setSort (new SortField (null, SortField.SCORE, true), SortField.FIELD_DOC );
    assertMatches (full, queryX, sort, "IEGCA");
    assertMatches (full, queryY, sort, "JFHDB");

    sort.setSort (new SortField (null, SortField.DOC, true));
    assertMatches (full, queryX, sort, "IGECA");
    assertMatches (full, queryY, sort, "JHFDB");

    sort.setSort (new SortField ("int", SortField.INT, true) );
    assertMatches (full, queryX, sort, "CAEGI");
    assertMatches (full, queryY, sort, "BJFHD");

    sort.setSort (new SortField ("float", SortField.FLOAT, true) );
    assertMatches (full, queryX, sort, "AECIG");
    assertMatches (full, queryY, sort, "BFJHD");

    sort.setSort (new SortField ("string", SortField.STRING, true) );
    assertMatches (full, queryX, sort, "CEGIA");
    assertMatches (full, queryY, sort, "BFHJD");
  }

  // test sorting when the sort field is empty (undefined) for some of the documents
  public void testEmptyFieldSort() throws Exception {
    sort.setSort (new SortField ("string", SortField.STRING) );
    assertMatches (full, queryF, sort, "ZJI");

    sort.setSort (new SortField ("string", SortField.STRING, true) );
    assertMatches (full, queryF, sort, "IJZ");
    
    sort.setSort (new SortField ("i18n", Locale.ENGLISH));
    assertMatches (full, queryF, sort, "ZJI");
    
    sort.setSort (new SortField ("i18n", Locale.ENGLISH, true));
    assertMatches (full, queryF, sort, "IJZ");

    sort.setSort (new SortField ("int", SortField.INT) );
    assertMatches (full, queryF, sort, "IZJ");

    sort.setSort (new SortField ("int", SortField.INT, true) );
    assertMatches (full, queryF, sort, "JZI");

    sort.setSort (new SortField ("float", SortField.FLOAT) );
    assertMatches (full, queryF, sort, "ZJI");

    // using a nonexisting field as first sort key shouldn't make a difference:
    sort.setSort (new SortField ("nosuchfield", SortField.STRING),
        new SortField ("float", SortField.FLOAT) );
    assertMatches (full, queryF, sort, "ZJI");

    sort.setSort (new SortField ("float", SortField.FLOAT, true) );
    assertMatches (full, queryF, sort, "IJZ");

    // When a field is null for both documents, the next SortField should be used.
                // Works for
    sort.setSort (new SortField ("int", SortField.INT),
                                new SortField ("string", SortField.STRING),
        new SortField ("float", SortField.FLOAT) );
    assertMatches (full, queryG, sort, "ZWXY");

    // Reverse the last criterium to make sure the test didn't pass by chance
    sort.setSort (new SortField ("int", SortField.INT),
                                new SortField ("string", SortField.STRING),
        new SortField ("float", SortField.FLOAT, true) );
    assertMatches (full, queryG, sort, "ZYXW");

    // Do the same for a MultiSearcher
    Searcher multiSearcher=new MultiSearcher (new Searchable[] { full });

    sort.setSort (new SortField ("int", SortField.INT),
                                new SortField ("string", SortField.STRING),
        new SortField ("float", SortField.FLOAT) );
    assertMatches (multiSearcher, queryG, sort, "ZWXY");

    sort.setSort (new SortField ("int", SortField.INT),
                                new SortField ("string", SortField.STRING),
        new SortField ("float", SortField.FLOAT, true) );
    assertMatches (multiSearcher, queryG, sort, "ZYXW");
    // Don't close the multiSearcher. it would close the full searcher too!

    // Do the same for a ParallelMultiSearcher
    ExecutorService exec = Executors.newFixedThreadPool(_TestUtil.nextInt(random, 2, 8));
    Searcher parallelSearcher=new ParallelMultiSearcher (exec, full);

    sort.setSort (new SortField ("int", SortField.INT),
                                new SortField ("string", SortField.STRING),
        new SortField ("float", SortField.FLOAT) );
    assertMatches (parallelSearcher, queryG, sort, "ZWXY");

    sort.setSort (new SortField ("int", SortField.INT),
                                new SortField ("string", SortField.STRING),
        new SortField ("float", SortField.FLOAT, true) );
    assertMatches (parallelSearcher, queryG, sort, "ZYXW");
    parallelSearcher.close();
    exec.awaitTermination(1000, TimeUnit.MILLISECONDS);
  }

  // test sorts using a series of fields
  public void testSortCombos() throws Exception {
    sort.setSort (new SortField ("int", SortField.INT), new SortField ("float", SortField.FLOAT) );
    assertMatches (full, queryX, sort, "IGEAC");

    sort.setSort (new SortField ("int", SortField.INT, true), new SortField (null, SortField.DOC, true) );
    assertMatches (full, queryX, sort, "CEAGI");

    sort.setSort (new SortField ("float", SortField.FLOAT), new SortField ("string", SortField.STRING) );
    assertMatches (full, queryX, sort, "GICEA");
  }

  // test using a Locale for sorting strings
  public void testLocaleSort() throws Exception {
    sort.setSort (new SortField ("string", Locale.US) );
    assertMatches (full, queryX, sort, "AIGEC");
    assertMatches (full, queryY, sort, "DJHFB");

    sort.setSort (new SortField ("string", Locale.US, true) );
    assertMatches (full, queryX, sort, "CEGIA");
    assertMatches (full, queryY, sort, "BFHJD");
  }

  // test using various international locales with accented characters
  // (which sort differently depending on locale)
  public void testInternationalSort() throws Exception {
    sort.setSort (new SortField ("i18n", Locale.US));
    assertMatches (full, queryY, sort, oStrokeFirst ? "BFJHD" : "BFJDH");

    sort.setSort (new SortField ("i18n", new Locale("sv", "se")));
    assertMatches (full, queryY, sort, "BJDFH");

    sort.setSort (new SortField ("i18n", new Locale("da", "dk")));
    assertMatches (full, queryY, sort, "BJDHF");

    sort.setSort (new SortField ("i18n", Locale.US));
    assertMatches (full, queryX, sort, "ECAGI");

    sort.setSort (new SortField ("i18n", Locale.FRANCE));
    assertMatches (full, queryX, sort, "EACGI");
  }
    
    // Test the MultiSearcher's ability to preserve locale-sensitive ordering
    // by wrapping it around a single searcher
  public void testInternationalMultiSearcherSort() throws Exception {
    Searcher multiSearcher = new MultiSearcher (new Searchable[] { full });
    
    sort.setSort (new SortField ("i18n", new Locale("sv", "se")));
    assertMatches (multiSearcher, queryY, sort, "BJDFH");
    
    sort.setSort (new SortField ("i18n", Locale.US));
    assertMatches (multiSearcher, queryY, sort, oStrokeFirst ? "BFJHD" : "BFJDH");
    
    sort.setSort (new SortField ("i18n", new Locale("da", "dk")));
    assertMatches (multiSearcher, queryY, sort, "BJDHF");
  } 

  // test a variety of sorts using more than one searcher
  public void testMultiSort() throws Exception {
    MultiSearcher searcher = new MultiSearcher (new Searchable[] { searchX, searchY });
    runMultiSorts(searcher, false);
  }

  // test a variety of sorts using a parallel multisearcher
  public void testParallelMultiSort() throws Exception {
    ExecutorService exec = Executors.newFixedThreadPool(_TestUtil.nextInt(random, 2, 8));
    Searcher searcher = new ParallelMultiSearcher (exec, searchX, searchY);
    runMultiSorts(searcher, false);
    searcher.close();
    exec.awaitTermination(1000, TimeUnit.MILLISECONDS);
  }

  // test that the relevancy scores are the same even if
  // hits are sorted
  public void testNormalizedScores() throws Exception {

    // capture relevancy scores
    HashMap<String,Float> scoresX = getScores (full.search (queryX, null, 1000).scoreDocs, full);
    HashMap<String,Float> scoresY = getScores (full.search (queryY, null, 1000).scoreDocs, full);
    HashMap<String,Float> scoresA = getScores (full.search (queryA, null, 1000).scoreDocs, full);

    // we'll test searching locally, remote and multi
    
    MultiSearcher multi  = new MultiSearcher (new Searchable[] { searchX, searchY });

    // change sorting and make sure relevancy stays the same

    sort = new Sort();
    assertSameValues (scoresX, getScores (full.search (queryX, null, 1000, sort).scoreDocs, full));
    assertSameValues (scoresX, getScores (multi.search (queryX, null, 1000, sort).scoreDocs, multi));
    assertSameValues (scoresY, getScores (full.search (queryY, null, 1000, sort).scoreDocs, full));
    assertSameValues (scoresY, getScores (multi.search (queryY, null, 1000, sort).scoreDocs, multi));
    assertSameValues (scoresA, getScores (full.search (queryA, null, 1000, sort).scoreDocs, full));
    assertSameValues (scoresA, getScores (multi.search (queryA, null, 1000, sort).scoreDocs, multi));

    sort.setSort(SortField.FIELD_DOC);
    assertSameValues (scoresX, getScores (full.search (queryX, null, 1000, sort).scoreDocs, full));
    assertSameValues (scoresX, getScores (multi.search (queryX, null, 1000, sort).scoreDocs, multi));
    assertSameValues (scoresY, getScores (full.search (queryY, null, 1000, sort).scoreDocs, full));
    assertSameValues (scoresY, getScores (multi.search (queryY, null, 1000, sort).scoreDocs, multi));
    assertSameValues (scoresA, getScores (full.search (queryA, null, 1000, sort).scoreDocs, full));
    assertSameValues (scoresA, getScores (multi.search (queryA, null, 1000, sort).scoreDocs, multi));

    sort.setSort (new SortField("int", SortField.INT));
    assertSameValues (scoresX, getScores (full.search (queryX, null, 1000, sort).scoreDocs, full));
    assertSameValues (scoresX, getScores (multi.search (queryX, null, 1000, sort).scoreDocs, multi));
    assertSameValues (scoresY, getScores (full.search (queryY, null, 1000, sort).scoreDocs, full));
    assertSameValues (scoresY, getScores (multi.search (queryY, null, 1000, sort).scoreDocs, multi));
    assertSameValues (scoresA, getScores (full.search (queryA, null, 1000, sort).scoreDocs, full));
    assertSameValues (scoresA, getScores (multi.search (queryA, null, 1000, sort).scoreDocs, multi));

    sort.setSort (new SortField("float", SortField.FLOAT));
    assertSameValues (scoresX, getScores (full.search (queryX, null, 1000, sort).scoreDocs, full));
    assertSameValues (scoresX, getScores (multi.search (queryX, null, 1000, sort).scoreDocs, multi));
    assertSameValues (scoresY, getScores (full.search (queryY, null, 1000, sort).scoreDocs, full));
    assertSameValues (scoresY, getScores (multi.search (queryY, null, 1000, sort).scoreDocs, multi));
    assertSameValues (scoresA, getScores (full.search (queryA, null, 1000, sort).scoreDocs, full));
    assertSameValues (scoresA, getScores (multi.search (queryA, null, 1000, sort).scoreDocs, multi));

    sort.setSort (new SortField("string", SortField.STRING));
    assertSameValues (scoresX, getScores (full.search (queryX, null, 1000, sort).scoreDocs, full));
    assertSameValues (scoresX, getScores (multi.search (queryX, null, 1000, sort).scoreDocs, multi));
    assertSameValues (scoresY, getScores (full.search (queryY, null, 1000, sort).scoreDocs, full));
    assertSameValues (scoresY, getScores (multi.search (queryY, null, 1000, sort).scoreDocs, multi));
    assertSameValues (scoresA, getScores (full.search (queryA, null, 1000, sort).scoreDocs, full));
    assertSameValues (scoresA, getScores (multi.search (queryA, null, 1000, sort).scoreDocs, multi));

    sort.setSort (new SortField("int", SortField.INT),new SortField("float", SortField.FLOAT));
    assertSameValues (scoresX, getScores (full.search (queryX, null, 1000, sort).scoreDocs, full));
    assertSameValues (scoresX, getScores (multi.search (queryX, null, 1000, sort).scoreDocs, multi));
    assertSameValues (scoresY, getScores (full.search (queryY, null, 1000, sort).scoreDocs, full));
    assertSameValues (scoresY, getScores (multi.search (queryY, null, 1000, sort).scoreDocs, multi));
    assertSameValues (scoresA, getScores (full.search (queryA, null, 1000, sort).scoreDocs, full));
    assertSameValues (scoresA, getScores (multi.search (queryA, null, 1000, sort).scoreDocs, multi));

    sort.setSort (new SortField ("int", SortField.INT, true), new SortField (null, SortField.DOC, true) );
    assertSameValues (scoresX, getScores (full.search (queryX, null, 1000, sort).scoreDocs, full));
    assertSameValues (scoresX, getScores (multi.search (queryX, null, 1000, sort).scoreDocs, multi));
    assertSameValues (scoresY, getScores (full.search (queryY, null, 1000, sort).scoreDocs, full));
    assertSameValues (scoresY, getScores (multi.search (queryY, null, 1000, sort).scoreDocs, multi));
    assertSameValues (scoresA, getScores (full.search (queryA, null, 1000, sort).scoreDocs, full));
    assertSameValues (scoresA, getScores (multi.search (queryA, null, 1000, sort).scoreDocs, multi));

    sort.setSort (new SortField("int", SortField.INT),new SortField("string", SortField.STRING));
    assertSameValues (scoresX, getScores (full.search (queryX, null, 1000, sort).scoreDocs, full));
    assertSameValues (scoresX, getScores (multi.search (queryX, null, 1000, sort).scoreDocs, multi));
    assertSameValues (scoresY, getScores (full.search (queryY, null, 1000, sort).scoreDocs, full));
    assertSameValues (scoresY, getScores (multi.search (queryY, null, 1000, sort).scoreDocs, multi));
    assertSameValues (scoresA, getScores (full.search (queryA, null, 1000, sort).scoreDocs, full));
    assertSameValues (scoresA, getScores (multi.search (queryA, null, 1000, sort).scoreDocs, multi));

  }

  public void testTopDocsScores() throws Exception {

    // There was previously a bug in FieldSortedHitQueue.maxscore when only a single
    // doc was added.  That is what the following tests for.
    Sort sort = new Sort();
    int nDocs=10;

    // try to pick a query that will result in an unnormalized
    // score greater than 1 to test for correct normalization
    final TopDocs docs1 = full.search(queryE,null,nDocs,sort);

    // a filter that only allows through the first hit
    Filter filt = new Filter() {
      @Override
      public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
        BitSet bs = new BitSet(reader.maxDoc());
        bs.set(0, reader.maxDoc());
        bs.set(docs1.scoreDocs[0].doc);
        return new DocIdBitSet(bs);
      }
    };

    TopDocs docs2 = full.search(queryE, filt, nDocs, sort);
    
    assertEquals(docs1.scoreDocs[0].score, docs2.scoreDocs[0].score, 1e-6);
  }
  
  public void testSortWithoutFillFields() throws Exception {
    
    // There was previously a bug in TopFieldCollector when fillFields was set
    // to false - the same doc and score was set in ScoreDoc[] array. This test
    // asserts that if fillFields is false, the documents are set properly. It
    // does not use Searcher's default search methods (with Sort) since all set
    // fillFields to true.
    Sort[] sort = new Sort[] { new Sort(SortField.FIELD_DOC), new Sort() };
    for (int i = 0; i < sort.length; i++) {
      Query q = new MatchAllDocsQuery();
      TopDocsCollector<Entry> tdc = TopFieldCollector.create(sort[i], 10, false,
          false, false, true);
      
      full.search(q, tdc);
      
      ScoreDoc[] sd = tdc.topDocs().scoreDocs;
      for (int j = 1; j < sd.length; j++) {
        assertTrue(sd[j].doc != sd[j - 1].doc);
      }
      
    }
  }

  public void testSortWithoutScoreTracking() throws Exception {

    // Two Sort criteria to instantiate the multi/single comparators.
    Sort[] sort = new Sort[] {new Sort(SortField.FIELD_DOC), new Sort() };
    for (int i = 0; i < sort.length; i++) {
      Query q = new MatchAllDocsQuery();
      TopDocsCollector<Entry> tdc = TopFieldCollector.create(sort[i], 10, true, false,
          false, true);
      
      full.search(q, tdc);
      
      TopDocs td = tdc.topDocs();
      ScoreDoc[] sd = td.scoreDocs;
      for (int j = 0; j < sd.length; j++) {
        assertTrue(Float.isNaN(sd[j].score));
      }
      assertTrue(Float.isNaN(td.getMaxScore()));
    }
  }
  
  public void testSortWithScoreNoMaxScoreTracking() throws Exception {
    
    // Two Sort criteria to instantiate the multi/single comparators.
    Sort[] sort = new Sort[] {new Sort(SortField.FIELD_DOC), new Sort() };
    for (int i = 0; i < sort.length; i++) {
      Query q = new MatchAllDocsQuery();
      TopDocsCollector<Entry> tdc = TopFieldCollector.create(sort[i], 10, true, true,
          false, true);
      
      full.search(q, tdc);
      
      TopDocs td = tdc.topDocs();
      ScoreDoc[] sd = td.scoreDocs;
      for (int j = 0; j < sd.length; j++) {
        assertTrue(!Float.isNaN(sd[j].score));
      }
      assertTrue(Float.isNaN(td.getMaxScore()));
    }
  }
  
  // MultiComparatorScoringNoMaxScoreCollector
  public void testSortWithScoreNoMaxScoreTrackingMulti() throws Exception {
    
    // Two Sort criteria to instantiate the multi/single comparators.
    Sort[] sort = new Sort[] {new Sort(SortField.FIELD_DOC, SortField.FIELD_SCORE) };
    for (int i = 0; i < sort.length; i++) {
      Query q = new MatchAllDocsQuery();
      TopDocsCollector<Entry> tdc = TopFieldCollector.create(sort[i], 10, true, true,
          false, true);

      full.search(q, tdc);
      
      TopDocs td = tdc.topDocs();
      ScoreDoc[] sd = td.scoreDocs;
      for (int j = 0; j < sd.length; j++) {
        assertTrue(!Float.isNaN(sd[j].score));
      }
      assertTrue(Float.isNaN(td.getMaxScore()));
    }
  }
  
  public void testSortWithScoreAndMaxScoreTracking() throws Exception {
    
    // Two Sort criteria to instantiate the multi/single comparators.
    Sort[] sort = new Sort[] {new Sort(SortField.FIELD_DOC), new Sort() };
    for (int i = 0; i < sort.length; i++) {
      Query q = new MatchAllDocsQuery();
      TopDocsCollector<Entry> tdc = TopFieldCollector.create(sort[i], 10, true, true,
          true, true);
      
      full.search(q, tdc);
      
      TopDocs td = tdc.topDocs();
      ScoreDoc[] sd = td.scoreDocs;
      for (int j = 0; j < sd.length; j++) {
        assertTrue(!Float.isNaN(sd[j].score));
      }
      assertTrue(!Float.isNaN(td.getMaxScore()));
    }
  }
  
  public void testOutOfOrderDocsScoringSort() throws Exception {

    // Two Sort criteria to instantiate the multi/single comparators.
    Sort[] sort = new Sort[] {new Sort(SortField.FIELD_DOC), new Sort() };
    boolean[][] tfcOptions = new boolean[][] {
        new boolean[] { false, false, false },
        new boolean[] { false, false, true },
        new boolean[] { false, true, false },
        new boolean[] { false, true, true },
        new boolean[] { true, false, false },
        new boolean[] { true, false, true },
        new boolean[] { true, true, false },
        new boolean[] { true, true, true },
    };
    String[] actualTFCClasses = new String[] {
        "OutOfOrderOneComparatorNonScoringCollector", 
        "OutOfOrderOneComparatorScoringMaxScoreCollector", 
        "OutOfOrderOneComparatorScoringNoMaxScoreCollector", 
        "OutOfOrderOneComparatorScoringMaxScoreCollector", 
        "OutOfOrderOneComparatorNonScoringCollector", 
        "OutOfOrderOneComparatorScoringMaxScoreCollector", 
        "OutOfOrderOneComparatorScoringNoMaxScoreCollector", 
        "OutOfOrderOneComparatorScoringMaxScoreCollector" 
    };
    
    BooleanQuery bq = new BooleanQuery();
    // Add a Query with SHOULD, since bw.scorer() returns BooleanScorer2
    // which delegates to BS if there are no mandatory clauses.
    bq.add(new MatchAllDocsQuery(), Occur.SHOULD);
    // Set minNrShouldMatch to 1 so that BQ will not optimize rewrite to return
    // the clause instead of BQ.
    bq.setMinimumNumberShouldMatch(1);
    for (int i = 0; i < sort.length; i++) {
      for (int j = 0; j < tfcOptions.length; j++) {
        TopDocsCollector<Entry> tdc = TopFieldCollector.create(sort[i], 10,
            tfcOptions[j][0], tfcOptions[j][1], tfcOptions[j][2], false);

        assertTrue(tdc.getClass().getName().endsWith("$"+actualTFCClasses[j]));
        
        full.search(bq, tdc);
        
        TopDocs td = tdc.topDocs();
        ScoreDoc[] sd = td.scoreDocs;
        assertEquals(10, sd.length);
      }
    }
  }
  
  // OutOfOrderMulti*Collector
  public void testOutOfOrderDocsScoringSortMulti() throws Exception {

    // Two Sort criteria to instantiate the multi/single comparators.
    Sort[] sort = new Sort[] {new Sort(SortField.FIELD_DOC, SortField.FIELD_SCORE) };
    boolean[][] tfcOptions = new boolean[][] {
        new boolean[] { false, false, false },
        new boolean[] { false, false, true },
        new boolean[] { false, true, false },
        new boolean[] { false, true, true },
        new boolean[] { true, false, false },
        new boolean[] { true, false, true },
        new boolean[] { true, true, false },
        new boolean[] { true, true, true },
    };
    String[] actualTFCClasses = new String[] {
        "OutOfOrderMultiComparatorNonScoringCollector", 
        "OutOfOrderMultiComparatorScoringMaxScoreCollector", 
        "OutOfOrderMultiComparatorScoringNoMaxScoreCollector", 
        "OutOfOrderMultiComparatorScoringMaxScoreCollector", 
        "OutOfOrderMultiComparatorNonScoringCollector", 
        "OutOfOrderMultiComparatorScoringMaxScoreCollector", 
        "OutOfOrderMultiComparatorScoringNoMaxScoreCollector", 
        "OutOfOrderMultiComparatorScoringMaxScoreCollector" 
    };
    
    BooleanQuery bq = new BooleanQuery();
    // Add a Query with SHOULD, since bw.scorer() returns BooleanScorer2
    // which delegates to BS if there are no mandatory clauses.
    bq.add(new MatchAllDocsQuery(), Occur.SHOULD);
    // Set minNrShouldMatch to 1 so that BQ will not optimize rewrite to return
    // the clause instead of BQ.
    bq.setMinimumNumberShouldMatch(1);
    for (int i = 0; i < sort.length; i++) {
      for (int j = 0; j < tfcOptions.length; j++) {
        TopDocsCollector<Entry> tdc = TopFieldCollector.create(sort[i], 10,
            tfcOptions[j][0], tfcOptions[j][1], tfcOptions[j][2], false);

        assertTrue(tdc.getClass().getName().endsWith("$"+actualTFCClasses[j]));
        
        full.search(bq, tdc);
        
        TopDocs td = tdc.topDocs();
        ScoreDoc[] sd = td.scoreDocs;
        assertEquals(10, sd.length);
      }
    }
  }
  
  public void testSortWithScoreAndMaxScoreTrackingNoResults() throws Exception {
    
    // Two Sort criteria to instantiate the multi/single comparators.
    Sort[] sort = new Sort[] {new Sort(SortField.FIELD_DOC), new Sort() };
    for (int i = 0; i < sort.length; i++) {
      TopDocsCollector<Entry> tdc = TopFieldCollector.create(sort[i], 10, true, true, true, true);
      TopDocs td = tdc.topDocs();
      assertEquals(0, td.totalHits);
      assertTrue(Float.isNaN(td.getMaxScore()));
    }
  }
  
  // runs a variety of sorts useful for multisearchers
  private void runMultiSorts(Searcher multi, boolean isFull) throws Exception {
    sort.setSort(SortField.FIELD_DOC);
    String expected = isFull ? "ABCDEFGHIJ" : "ACEGIBDFHJ";
    assertMatches(multi, queryA, sort, expected);

    sort.setSort(new SortField ("int", SortField.INT));
    expected = isFull ? "IDHFGJABEC" : "IDHFGJAEBC";
    assertMatches(multi, queryA, sort, expected);

    sort.setSort(new SortField ("int", SortField.INT), SortField.FIELD_DOC);
    expected = isFull ? "IDHFGJABEC" : "IDHFGJAEBC";
    assertMatches(multi, queryA, sort, expected);

    sort.setSort(new SortField("int", SortField.INT));
    expected = isFull ? "IDHFGJABEC" : "IDHFGJAEBC";
    assertMatches(multi, queryA, sort, expected);

    sort.setSort(new SortField ("float", SortField.FLOAT), SortField.FIELD_DOC);
    assertMatches(multi, queryA, sort, "GDHJCIEFAB");

    sort.setSort(new SortField("float", SortField.FLOAT));
    assertMatches(multi, queryA, sort, "GDHJCIEFAB");

    sort.setSort(new SortField("string", SortField.STRING));
    assertMatches(multi, queryA, sort, "DJAIHGFEBC");

    sort.setSort(new SortField("int", SortField.INT, true));
    expected = isFull ? "CABEJGFHDI" : "CAEBJGFHDI";
    assertMatches(multi, queryA, sort, expected);

    sort.setSort(new SortField("float", SortField.FLOAT, true));
    assertMatches(multi, queryA, sort, "BAFECIJHDG");

    sort.setSort(new SortField("string", SortField.STRING, true));
    assertMatches(multi, queryA, sort, "CBEFGHIAJD");

    sort.setSort(new SortField("int", SortField.INT),new SortField("float", SortField.FLOAT));
    assertMatches(multi, queryA, sort, "IDHFGJEABC");

    sort.setSort(new SortField("float", SortField.FLOAT),new SortField("string", SortField.STRING));
    assertMatches(multi, queryA, sort, "GDHJICEFAB");

    sort.setSort(new SortField ("int", SortField.INT));
    assertMatches(multi, queryF, sort, "IZJ");

    sort.setSort(new SortField ("int", SortField.INT, true));
    assertMatches(multi, queryF, sort, "JZI");

    sort.setSort(new SortField ("float", SortField.FLOAT));
    assertMatches(multi, queryF, sort, "ZJI");

    sort.setSort(new SortField ("string", SortField.STRING));
    assertMatches(multi, queryF, sort, "ZJI");

    sort.setSort(new SortField ("string", SortField.STRING, true));
    assertMatches(multi, queryF, sort, "IJZ");

    // up to this point, all of the searches should have "sane" 
    // FieldCache behavior, and should have reused hte cache in several cases
    assertSaneFieldCaches(getName() + " various");
    // next we'll check Locale based (String[]) for 'string', so purge first
    FieldCache.DEFAULT.purgeAllCaches();

    sort.setSort(new SortField ("string", Locale.US) );
    assertMatches(multi, queryA, sort, "DJAIHGFEBC");

    sort.setSort(new SortField ("string", Locale.US, true) );
    assertMatches(multi, queryA, sort, "CBEFGHIAJD");

    sort.setSort(new SortField ("string", Locale.UK) );
    assertMatches(multi, queryA, sort, "DJAIHGFEBC");

    assertSaneFieldCaches(getName() + " Locale.US + Locale.UK");
    FieldCache.DEFAULT.purgeAllCaches();

  }

  private void assertMatches(Searcher searcher, Query query, Sort sort, String expectedResult) throws IOException {
    assertMatches( null, searcher, query, sort, expectedResult );
  }

  // make sure the documents returned by the search match the expected list
  private void assertMatches(String msg, Searcher searcher, Query query, Sort sort,
      String expectedResult) throws IOException {
    //ScoreDoc[] result = searcher.search (query, null, 1000, sort).scoreDocs;
    TopDocs hits = searcher.search (query, null, Math.max(1, expectedResult.length()), sort);
    ScoreDoc[] result = hits.scoreDocs;
    assertEquals(expectedResult.length(),hits.totalHits);
    StringBuilder buff = new StringBuilder(10);
    int n = result.length;
    for (int i=0; i<n; ++i) {
      Document doc = searcher.doc(result[i].doc);
      String[] v = doc.getValues("tracer");
      for (int j=0; j<v.length; ++j) {
        buff.append (v[j]);
      }
    }
    assertEquals (msg, expectedResult, buff.toString());
  }

  private HashMap<String,Float> getScores (ScoreDoc[] hits, Searcher searcher)
  throws IOException {
    HashMap<String,Float> scoreMap = new HashMap<String,Float>();
    int n = hits.length;
    for (int i=0; i<n; ++i) {
      Document doc = searcher.doc(hits[i].doc);
      String[] v = doc.getValues("tracer");
      assertEquals (v.length, 1);
      scoreMap.put (v[0], Float.valueOf(hits[i].score));
    }
    return scoreMap;
  }

  // make sure all the values in the maps match
  private <K, V> void assertSameValues (HashMap<K,V> m1, HashMap<K,V> m2) {
    int n = m1.size();
    int m = m2.size();
    assertEquals (n, m);
    Iterator<K> iter = m1.keySet().iterator();
    while (iter.hasNext()) {
      K key = iter.next();
      V o1 = m1.get(key);
      V o2 = m2.get(key);
      if (o1 instanceof Float) {
        assertEquals(((Float)o1).floatValue(), ((Float)o2).floatValue(), 1e-6);
      } else {
        assertEquals (m1.get(key), m2.get(key));
      }
    }
  }

  public void testEmptyStringVsNullStringSort() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(
                        TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    Document doc = new Document();
    doc.add(newField("f", "", Field.Store.NO, Field.Index.NOT_ANALYZED));
    doc.add(newField("t", "1", Field.Store.NO, Field.Index.NOT_ANALYZED));
    w.addDocument(doc);
    w.commit();
    doc = new Document();
    doc.add(newField("t", "1", Field.Store.NO, Field.Index.NOT_ANALYZED));
    w.addDocument(doc);

    IndexReader r = IndexReader.open(w, true);
    w.close();
    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(new TermQuery(new Term("t", "1")), null, 10, new Sort(new SortField("f", SortField.STRING)));
    assertEquals(2, hits.totalHits);
    // null sorts first
    assertEquals(1, hits.scoreDocs[0].doc);
    assertEquals(0, hits.scoreDocs[1].doc);
    s.close();
    r.close();
    dir.close();
  }

  public void testLUCENE2142() throws IOException {
    Directory indexStore = newDirectory();
    IndexWriter writer = new IndexWriter(indexStore, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    for (int i=0; i<5; i++) {
        Document doc = new Document();
        doc.add (new Field ("string", "a"+i, Field.Store.NO, Field.Index.NOT_ANALYZED));
        doc.add (new Field ("string", "b"+i, Field.Store.NO, Field.Index.NOT_ANALYZED));
        writer.addDocument (doc);
    }
    writer.forceMerge(1); // enforce one segment to have a higher unique term count in all cases
    writer.close();
    sort.setSort(
        new SortField("string", SortField.STRING),
        SortField.FIELD_DOC );
    // this should not throw AIOOBE or RuntimeEx
    IndexReader reader = IndexReader.open(indexStore);
    IndexSearcher searcher = new IndexSearcher(reader);
    searcher.search(new MatchAllDocsQuery(), null, 500, sort);
    searcher.close();
    reader.close();
    indexStore.close();
  }

  public void testCountingCollector() throws Exception {
    Directory indexStore = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random, indexStore);
    for (int i=0; i<5; i++) {
      Document doc = new Document();
      doc.add (new Field ("string", "a"+i, Field.Store.NO, Field.Index.NOT_ANALYZED));
      doc.add (new Field ("string", "b"+i, Field.Store.NO, Field.Index.NOT_ANALYZED));
      writer.addDocument (doc);
    }
    IndexReader reader = writer.getReader();
    writer.close();

    IndexSearcher searcher = newSearcher(reader);
    TotalHitCountCollector c = new TotalHitCountCollector();
    searcher.search(new MatchAllDocsQuery(), null, c);
    assertEquals(5, c.getTotalHits());
    searcher.close();
    reader.close();
    indexStore.close();
  }
  
  /** LUCENE-3390: expose bug in first round of that issue */
  public void testSimultaneousSorts() throws IOException {
    Sort sortMin = new Sort(new SortField ("int", SortField.INT, false).setMissingValue(new Integer(Integer.MIN_VALUE)));
    Sort sortMax = new Sort(new SortField ("int", SortField.INT, false).setMissingValue(new Integer(Integer.MAX_VALUE)));
    Sort sortMinRev = new Sort(new SortField ("int", SortField.INT, true).setMissingValue(new Integer(Integer.MIN_VALUE)));
    Sort sortMaxRev = new Sort(new SortField ("int", SortField.INT, true).setMissingValue(new Integer(Integer.MAX_VALUE)));
    
    int ndocs = full.maxDoc();
    TopFieldCollector collectorMin = TopFieldCollector.create(sortMin, ndocs, false, false, false, true);
    TopFieldCollector collectorMax = TopFieldCollector.create(sortMax, ndocs, false, false, false, true);
    TopFieldCollector collectorMinRev = TopFieldCollector.create(sortMinRev, ndocs, false, false, false, true);
    TopFieldCollector collectorMaxRev = TopFieldCollector.create(sortMaxRev, ndocs, false, false, false, true);
    full.search(new MatchAllDocsQuery(), MultiCollector.wrap(collectorMin, collectorMax, collectorMinRev, collectorMaxRev));
    
    assertIntResultsOrder(collectorMin, ndocs, false, Integer.MIN_VALUE);
    assertIntResultsOrder(collectorMax, ndocs, false, Integer.MAX_VALUE);
    assertIntResultsOrder(collectorMinRev, ndocs, true, Integer.MIN_VALUE);
    assertIntResultsOrder(collectorMaxRev, ndocs, true, Integer.MAX_VALUE);
  }

  private void assertIntResultsOrder(TopFieldCollector collector, int ndocs, boolean reverse, int missingVal) {
    ScoreDoc[] fdocs = collector.topDocs().scoreDocs;
    assertEquals("wrong number of docs collected", ndocs, fdocs.length);
    int b = dataIntVal(fdocs[0].doc, missingVal);
    for (int i=1; i<fdocs.length; i++) {
      int a = b;
      b = dataIntVal(fdocs[i].doc, missingVal);
      if (reverse) {
        // reverse of natural int order: descending
        assertTrue(a >= b);
      } else {
        // natural int order: ascending
        assertTrue( b >= a);
      }
    }
  }

  private int dataIntVal(int doc, int missingVal) {
    return data[doc][2]==null ? missingVal : Integer.parseInt(data[doc][2]);
  }

}
