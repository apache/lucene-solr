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
import java.util.ArrayList;
import java.util.BitSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.IndexDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.codecs.CodecProvider;
import org.apache.lucene.index.values.ValueType;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.FieldValueHitQueue.Entry;
import org.apache.lucene.search.cache.ByteValuesCreator;
import org.apache.lucene.search.cache.CachedArrayCreator;
import org.apache.lucene.search.cache.DoubleValuesCreator;
import org.apache.lucene.search.cache.FloatValuesCreator;
import org.apache.lucene.search.cache.IntValuesCreator;
import org.apache.lucene.search.cache.LongValuesCreator;
import org.apache.lucene.search.cache.ShortValuesCreator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
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

public class TestSort extends LuceneTestCase {
  // true if our codec supports docvalues: true unless codec is preflex (3.x)
  boolean supportsDocValues = CodecProvider.getDefault().getDefaultFieldCodec().equals("PreFlex") == false;
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
  
  // create an index of all the documents, or just the x, or just the y documents
  private IndexSearcher getIndex (boolean even, boolean odd)
  throws IOException {
    Directory indexStore = newDirectory();
    dirs.add(indexStore);
    RandomIndexWriter writer = new RandomIndexWriter(random, indexStore, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMergePolicy(newLogMergePolicy()));

    FieldType ft1 = new FieldType();
    ft1.setStored(true);
    FieldType ft2 = new FieldType();
    ft2.setIndexed(true);
    for (int i=0; i<data.length; ++i) {
      if (((i%2)==0 && even) || ((i%2)==1 && odd)) {
        Document doc = new Document();
        doc.add (new Field ("tracer", data[i][0], ft1));
        doc.add (new TextField ("contents", data[i][1]));
        if (data[i][2] != null) {
          Field f = new StringField ("int", data[i][2]);
          if (supportsDocValues) {
            f = IndexDocValuesField.build(f, ValueType.VAR_INTS);
          };
          doc.add(f);
        }
        if (data[i][3] != null) {
          Field f = new StringField ("float", data[i][3]);
          if (supportsDocValues) {
            f = IndexDocValuesField.build(f, ValueType.FLOAT_32);
          }
          doc.add(f);
        }
        if (data[i][4] != null) doc.add (new StringField ("string",   data[i][4]));
        if (data[i][5] != null) doc.add (new StringField ("custom",   data[i][5]));
        if (data[i][6] != null) doc.add (new StringField ("i18n",     data[i][6]));
        if (data[i][7] != null) doc.add (new StringField ("long",     data[i][7]));
        if (data[i][8] != null) {
          Field f = new StringField ("double", data[i][8]);
          if (supportsDocValues) {
            f = IndexDocValuesField.build(f, ValueType.FLOAT_64);
          }
          doc.add(f);
        }
        if (data[i][9] != null) doc.add (new StringField ("short",     data[i][9]));
        if (data[i][10] != null) doc.add (new StringField ("byte",     data[i][10]));
        if (data[i][11] != null) doc.add (new StringField ("parser",     data[i][11]));

        for(IndexableField f : doc.getFields()) {
          ((Field) f).setBoost(2.0f);
        }

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
    FieldType customType = new FieldType();
    customType.setStored(true);
    for (int i=0; i<NUM_STRINGS; i++) {
        Document doc = new Document();
        String num = getRandomCharString(getRandomNumber(2, 8), 48, 52);
        doc.add (new Field ("tracer", num, customType));
        //doc.add (new Field ("contents", Integer.toString(i), Field.Store.NO, Field.Index.ANALYZED));
        doc.add (new StringField ("string", num));
        String num2 = getRandomCharString(getRandomNumber(1, 4), 48, 50);
        doc.add (new StringField ("string2", num2));
        doc.add (new Field ("tracer2", num2, customType));
        for(IndexableField f : doc.getFields()) {
          ((Field) f).setBoost(2.0f);
        }
        writer.addDocument (doc);
    }
    //writer.optimize ();
    //System.out.println(writer.getSegmentCount());
    writer.close ();
    return new IndexSearcher (indexStore, true);
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

  private static SortField useDocValues(SortField field) {
    field.setUseIndexValues(true);
    return field;
  }
  // test sorts where the type of field is specified
  public void testTypedSort() throws Exception {
    sort.setSort (new SortField ("int", SortField.Type.INT), SortField.FIELD_DOC );
    assertMatches (full, queryX, sort, "IGAEC");
    assertMatches (full, queryY, sort, "DHFJB");
    
    sort.setSort (new SortField ("float", SortField.Type.FLOAT), SortField.FIELD_DOC );
    assertMatches (full, queryX, sort, "GCIEA");
    assertMatches (full, queryY, sort, "DHJFB");

    sort.setSort (new SortField ("long", SortField.Type.LONG), SortField.FIELD_DOC );
    assertMatches (full, queryX, sort, "EACGI");
    assertMatches (full, queryY, sort, "FBJHD");

    sort.setSort (new SortField ("double", SortField.Type.DOUBLE), SortField.FIELD_DOC );
    assertMatches (full, queryX, sort, "AGICE");
    assertMatches (full, queryY, sort, "DJHBF");
    
    sort.setSort (new SortField ("byte", SortField.Type.BYTE), SortField.FIELD_DOC );
    assertMatches (full, queryX, sort, "CIGAE");
    assertMatches (full, queryY, sort, "DHFBJ");

    sort.setSort (new SortField ("short", SortField.Type.SHORT), SortField.FIELD_DOC );
    assertMatches (full, queryX, sort, "IAGCE");
    assertMatches (full, queryY, sort, "DFHBJ");

    sort.setSort (new SortField ("string", SortField.Type.STRING), SortField.FIELD_DOC );
    assertMatches (full, queryX, sort, "AIGEC");
    assertMatches (full, queryY, sort, "DJHFB");
    
    if (supportsDocValues) {
      sort.setSort (useDocValues(new SortField ("int", SortField.Type.INT)), SortField.FIELD_DOC );
      assertMatches (full, queryX, sort, "IGAEC");
      assertMatches (full, queryY, sort, "DHFJB");
      
      sort.setSort (useDocValues(new SortField ("float", SortField.Type.FLOAT)), SortField.FIELD_DOC );
      assertMatches (full, queryX, sort, "GCIEA");
      assertMatches (full, queryY, sort, "DHJFB");
      
      sort.setSort (useDocValues(new SortField ("double", SortField.Type.DOUBLE)), SortField.FIELD_DOC );
      assertMatches (full, queryX, sort, "AGICE");
      assertMatches (full, queryY, sort, "DJHBF");
    }
  }
  
  private static class SortMissingLastTestHelper {
    CachedArrayCreator<?> creator;
    Object min;
    Object max;
    
    SortMissingLastTestHelper( CachedArrayCreator<?> c, Object min, Object max ) {
      creator = c;
      this.min = min;
      this.max = max;
    }
  }

  // test sorts where the type of field is specified
  public void testSortMissingLast() throws Exception {
    
    SortMissingLastTestHelper[] testers = new SortMissingLastTestHelper[] {
        new SortMissingLastTestHelper( new ByteValuesCreator(   "byte",   null ), Byte.MIN_VALUE,    Byte.MAX_VALUE ),
        new SortMissingLastTestHelper( new ShortValuesCreator(  "short",  null ), Short.MIN_VALUE,   Short.MAX_VALUE ),
        new SortMissingLastTestHelper( new IntValuesCreator(    "int",    null ), Integer.MIN_VALUE, Integer.MAX_VALUE ),
        new SortMissingLastTestHelper( new LongValuesCreator(   "long",   null ), Long.MIN_VALUE,    Long.MAX_VALUE ),
        new SortMissingLastTestHelper( new FloatValuesCreator(  "float",  null ), Float.MIN_VALUE,   Float.MAX_VALUE ),
        new SortMissingLastTestHelper( new DoubleValuesCreator( "double", null ), Double.MIN_VALUE,  Double.MAX_VALUE ),
    };
    
    for( SortMissingLastTestHelper t : testers ) {
      sort.setSort (new SortField( t.creator, false ), SortField.FIELD_DOC );
      assertMatches("creator:"+t.creator, full, queryM, sort, "adbc" );

      sort.setSort (new SortField( t.creator, false ).setMissingValue( t.max ), SortField.FIELD_DOC );
      assertMatches("creator:"+t.creator, full, queryM, sort, "bcad" );

      sort.setSort (new SortField( t.creator, false ).setMissingValue( t.min ), SortField.FIELD_DOC );
      assertMatches("creator:"+t.creator, full, queryM, sort, "adbc" );
    }
  }
  
  /**
   * Test String sorting: small queue to many matches, multi field sort, reverse sort
   */
  public void testStringSort() throws IOException {
    ScoreDoc[] result = null;
    IndexSearcher searcher = getFullStrings();
    sort.setSort(
        new SortField("string", SortField.Type.STRING),
        new SortField("string2", SortField.Type.STRING, true),
        SortField.FIELD_DOC);

    result = searcher.search(new MatchAllDocsQuery(), null, 500, sort).scoreDocs;

    StringBuilder buff = new StringBuilder();
    int n = result.length;
    String last = null;
    String lastSub = null;
    int lastDocId = 0;
    boolean fail = false;
    for (int x = 0; x < n; ++x) {
      Document doc2 = searcher.doc(result[x].doc);
      IndexableField[] v = doc2.getFields("tracer");
      IndexableField[] v2 = doc2.getFields("tracer2");
      for (int j = 0; j < v.length; ++j) {
        if (last != null) {
          int cmp = v[j].stringValue().compareTo(last);
          if (!(cmp >= 0)) { // ensure first field is in order
            fail = true;
            System.out.println("fail:" + v[j] + " < " + last);
          }
          if (cmp == 0) { // ensure second field is in reverse order
            cmp = v2[j].stringValue().compareTo(lastSub);
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
        last = v[j].stringValue();
        lastSub = v2[j].stringValue();
        lastDocId = result[x].doc;
        buff.append(v[j] + "(" + v2[j] + ")(" + result[x].doc+") ");
      }
    }
    if(fail) {
      System.out.println("topn field1(field2)(docID):" + buff);
    }
    assertFalse("Found sort results out of order", fail);
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
      public final int parseInt(final BytesRef term) {
        return (term.bytes[term.offset]-'A') * 123456;
      }
    }), SortField.FIELD_DOC );
    assertMatches (full, queryA, sort, "JIHGFEDCBA");
    assertSaneFieldCaches(getName() + " IntParser");
    fc.purgeAllCaches();

    sort.setSort (new SortField ("parser", new FieldCache.FloatParser(){
      public final float parseFloat(final BytesRef term) {
        return (float) Math.sqrt( term.bytes[term.offset] );
      }
    }), SortField.FIELD_DOC );
    assertMatches (full, queryA, sort, "JIHGFEDCBA");
    assertSaneFieldCaches(getName() + " FloatParser");
    fc.purgeAllCaches();

    sort.setSort (new SortField ("parser", new FieldCache.LongParser(){
      public final long parseLong(final BytesRef term) {
        return (term.bytes[term.offset]-'A') * 1234567890L;
      }
    }), SortField.FIELD_DOC );
    assertMatches (full, queryA, sort, "JIHGFEDCBA");
    assertSaneFieldCaches(getName() + " LongParser");
    fc.purgeAllCaches();

    sort.setSort (new SortField ("parser", new FieldCache.DoubleParser(){
      public final double parseDouble(final BytesRef term) {
        return Math.pow( term.bytes[term.offset], (term.bytes[term.offset]-'A') );
      }
    }), SortField.FIELD_DOC );
    assertMatches (full, queryA, sort, "JIHGFEDCBA");
    assertSaneFieldCaches(getName() + " DoubleParser");
    fc.purgeAllCaches();

    sort.setSort (new SortField ("parser", new FieldCache.ByteParser(){
      public final byte parseByte(final BytesRef term) {
        return (byte) (term.bytes[term.offset]-'A');
      }
    }), SortField.FIELD_DOC );
    assertMatches (full, queryA, sort, "JIHGFEDCBA");
    assertSaneFieldCaches(getName() + " ByteParser");
    fc.purgeAllCaches();

    sort.setSort (new SortField ("parser", new FieldCache.ShortParser(){
      public final short parseShort(final BytesRef term) {
        return (short) (term.bytes[term.offset]-'A');
      }
    }), SortField.FIELD_DOC );
    assertMatches (full, queryA, sort, "JIHGFEDCBA");
    assertSaneFieldCaches(getName() + " ShortParser");
    fc.purgeAllCaches();
  }

  // test sorts when there's nothing in the index
  public void testEmptyIndex() throws Exception {
    IndexSearcher empty = getEmptyIndex();

    sort = new Sort();
    assertMatches (empty, queryX, sort, "");

    sort.setSort(SortField.FIELD_DOC);
    assertMatches (empty, queryX, sort, "");

    sort.setSort (new SortField ("int", SortField.Type.INT), SortField.FIELD_DOC );
    assertMatches (empty, queryX, sort, "");
    
    sort.setSort (useDocValues(new SortField ("int", SortField.Type.INT)), SortField.FIELD_DOC );
    assertMatches (empty, queryX, sort, "");

    sort.setSort (new SortField ("string", SortField.Type.STRING, true), SortField.FIELD_DOC );
    assertMatches (empty, queryX, sort, "");

    sort.setSort (new SortField ("float", SortField.Type.FLOAT), new SortField ("string", SortField.Type.STRING) );
    assertMatches (empty, queryX, sort, "");
    
    sort.setSort (useDocValues(new SortField ("float", SortField.Type.FLOAT)), new SortField ("string", SortField.Type.STRING) );
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
      public final int parseInt(final BytesRef term) {
        return (term.bytes[term.offset]-'A') * 123456;
      }
    };

    @Override
    public FieldComparator setNextReader(AtomicReaderContext context) throws IOException {
      docValues = FieldCache.DEFAULT.getInts(context.reader, "parser", testIntParser);
      return this;
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
    sort.setSort (new SortField (null, SortField.Type.SCORE, true), SortField.FIELD_DOC );
    assertMatches (full, queryX, sort, "IEGCA");
    assertMatches (full, queryY, sort, "JFHDB");

    sort.setSort (new SortField (null, SortField.Type.DOC, true));
    assertMatches (full, queryX, sort, "IGECA");
    assertMatches (full, queryY, sort, "JHFDB");

    sort.setSort (new SortField ("int", SortField.Type.INT, true) );
    assertMatches (full, queryX, sort, "CAEGI");
    assertMatches (full, queryY, sort, "BJFHD");

    sort.setSort (new SortField ("float", SortField.Type.FLOAT, true) );
    assertMatches (full, queryX, sort, "AECIG");
    assertMatches (full, queryY, sort, "BFJHD");
    
    sort.setSort (new SortField ("string", SortField.Type.STRING, true) );
    assertMatches (full, queryX, sort, "CEGIA");
    assertMatches (full, queryY, sort, "BFHJD");
    
    if (supportsDocValues) {
      sort.setSort (useDocValues(new SortField ("int", SortField.Type.INT, true)) );
      assertMatches (full, queryX, sort, "CAEGI");
      assertMatches (full, queryY, sort, "BJFHD");
    
      sort.setSort (useDocValues(new SortField ("float", SortField.Type.FLOAT, true)) );
      assertMatches (full, queryX, sort, "AECIG");
      assertMatches (full, queryY, sort, "BFJHD");
    }
  }

  // test sorting when the sort field is empty (undefined) for some of the documents
  public void testEmptyFieldSort() throws Exception {
    sort.setSort (new SortField ("string", SortField.Type.STRING) );
    assertMatches (full, queryF, sort, "ZJI");

    sort.setSort (new SortField ("string", SortField.Type.STRING, true) );
    assertMatches (full, queryF, sort, "IJZ");
    
    sort.setSort (new SortField ("int", SortField.Type.INT) );
    assertMatches (full, queryF, sort, "IZJ");

    sort.setSort (new SortField ("int", SortField.Type.INT, true) );
    assertMatches (full, queryF, sort, "JZI");

    sort.setSort (new SortField ("float", SortField.Type.FLOAT) );
    assertMatches (full, queryF, sort, "ZJI");

    if (supportsDocValues) {
      sort.setSort (useDocValues(new SortField ("int", SortField.Type.INT)) );
      assertMatches (full, queryF, sort, "IZJ");
    
      sort.setSort (useDocValues(new SortField ("float", SortField.Type.FLOAT)) );
      assertMatches (full, queryF, sort, "ZJI");
    }

    // using a nonexisting field as first sort key shouldn't make a difference:
    sort.setSort (new SortField ("nosuchfield", SortField.Type.STRING),
        new SortField ("float", SortField.Type.FLOAT) );
    assertMatches (full, queryF, sort, "ZJI");

    sort.setSort (new SortField ("float", SortField.Type.FLOAT, true) );
    assertMatches (full, queryF, sort, "IJZ");

    // When a field is null for both documents, the next SortField should be used.
                // Works for
    sort.setSort (new SortField ("int", SortField.Type.INT),
                                new SortField ("string", SortField.Type.STRING),
        new SortField ("float", SortField.Type.FLOAT) );
    assertMatches (full, queryG, sort, "ZWXY");

    // Reverse the last criterium to make sure the test didn't pass by chance
    sort.setSort (new SortField ("int", SortField.Type.INT),
                                new SortField ("string", SortField.Type.STRING),
        new SortField ("float", SortField.Type.FLOAT, true) );
    assertMatches (full, queryG, sort, "ZYXW");

    // Do the same for a ParallelMultiSearcher
    ExecutorService exec = Executors.newFixedThreadPool(_TestUtil.nextInt(random, 2, 8));
    IndexSearcher parallelSearcher=new IndexSearcher (full.getIndexReader(), exec);

    sort.setSort (new SortField ("int", SortField.Type.INT),
                                new SortField ("string", SortField.Type.STRING),
        new SortField ("float", SortField.Type.FLOAT) );
    assertMatches (parallelSearcher, queryG, sort, "ZWXY");

    sort.setSort (new SortField ("int", SortField.Type.INT),
                                new SortField ("string", SortField.Type.STRING),
        new SortField ("float", SortField.Type.FLOAT, true) );
    assertMatches (parallelSearcher, queryG, sort, "ZYXW");
    parallelSearcher.close();
    exec.shutdown();
    exec.awaitTermination(1000, TimeUnit.MILLISECONDS);
  }

  // test sorts using a series of fields
  public void testSortCombos() throws Exception {
    sort.setSort (new SortField ("int", SortField.Type.INT), new SortField ("float", SortField.Type.FLOAT) );
    assertMatches (full, queryX, sort, "IGEAC");

    sort.setSort (new SortField ("int", SortField.Type.INT, true), new SortField (null, SortField.Type.DOC, true) );
    assertMatches (full, queryX, sort, "CEAGI");

    sort.setSort (new SortField ("float", SortField.Type.FLOAT), new SortField ("string", SortField.Type.STRING) );
    assertMatches (full, queryX, sort, "GICEA");
  }

  // test a variety of sorts using a parallel multisearcher
  public void testParallelMultiSort() throws Exception {
    ExecutorService exec = Executors.newFixedThreadPool(_TestUtil.nextInt(random, 2, 8));
    IndexSearcher searcher = new IndexSearcher(
                                  new MultiReader(
                                       new IndexReader[] {searchX.getIndexReader(),
                                                          searchY.getIndexReader()}), exec);
    runMultiSorts(searcher, false);
    searcher.close();
    exec.shutdown();
    exec.awaitTermination(1000, TimeUnit.MILLISECONDS);
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
      public DocIdSet getDocIdSet (AtomicReaderContext context, Bits acceptDocs) {
        assertNull("acceptDocs should be null, as we have no deletions", acceptDocs);
        BitSet bs = new BitSet(context.reader.maxDoc());
        bs.set(0, context.reader.maxDoc());
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
  private void runMultiSorts(IndexSearcher multi, boolean isFull) throws Exception {
    sort.setSort(SortField.FIELD_DOC);
    String expected = isFull ? "ABCDEFGHIJ" : "ACEGIBDFHJ";
    assertMatches(multi, queryA, sort, expected);

    sort.setSort(new SortField ("int", SortField.Type.INT));
    expected = isFull ? "IDHFGJABEC" : "IDHFGJAEBC";
    assertMatches(multi, queryA, sort, expected);

    sort.setSort(new SortField ("int", SortField.Type.INT), SortField.FIELD_DOC);
    expected = isFull ? "IDHFGJABEC" : "IDHFGJAEBC";
    assertMatches(multi, queryA, sort, expected);

    sort.setSort(new SortField("int", SortField.Type.INT));
    expected = isFull ? "IDHFGJABEC" : "IDHFGJAEBC";
    assertMatches(multi, queryA, sort, expected);
    
    sort.setSort(new SortField ("float", SortField.Type.FLOAT), SortField.FIELD_DOC);
    assertMatches(multi, queryA, sort, "GDHJCIEFAB");

    sort.setSort(new SortField("float", SortField.Type.FLOAT));
    assertMatches(multi, queryA, sort, "GDHJCIEFAB");

    sort.setSort(new SortField("string", SortField.Type.STRING));
    assertMatches(multi, queryA, sort, "DJAIHGFEBC");

    sort.setSort(new SortField("int", SortField.Type.INT, true));
    expected = isFull ? "CABEJGFHDI" : "CAEBJGFHDI";
    assertMatches(multi, queryA, sort, expected);

    sort.setSort(new SortField("float", SortField.Type.FLOAT, true));
    assertMatches(multi, queryA, sort, "BAFECIJHDG");

    sort.setSort(new SortField("string", SortField.Type.STRING, true));
    assertMatches(multi, queryA, sort, "CBEFGHIAJD");

    sort.setSort(new SortField("int", SortField.Type.INT),new SortField("float", SortField.Type.FLOAT));
    assertMatches(multi, queryA, sort, "IDHFGJEABC");

    sort.setSort(new SortField("float", SortField.Type.FLOAT),new SortField("string", SortField.Type.STRING));
    assertMatches(multi, queryA, sort, "GDHJICEFAB");

    sort.setSort(new SortField ("int", SortField.Type.INT));
    assertMatches(multi, queryF, sort, "IZJ");

    sort.setSort(new SortField ("int", SortField.Type.INT, true));
    assertMatches(multi, queryF, sort, "JZI");

    sort.setSort(new SortField ("float", SortField.Type.FLOAT));
    assertMatches(multi, queryF, sort, "ZJI");

    sort.setSort(new SortField ("string", SortField.Type.STRING));
    assertMatches(multi, queryF, sort, "ZJI");

    sort.setSort(new SortField ("string", SortField.Type.STRING, true));
    assertMatches(multi, queryF, sort, "IJZ");

    if (supportsDocValues) {
      sort.setSort(useDocValues(new SortField ("int", SortField.Type.INT)));
      expected = isFull ? "IDHFGJABEC" : "IDHFGJAEBC";
      assertMatches(multi, queryA, sort, expected);

      sort.setSort(useDocValues(new SortField ("int", SortField.Type.INT)), SortField.FIELD_DOC);
      expected = isFull ? "IDHFGJABEC" : "IDHFGJAEBC";
      assertMatches(multi, queryA, sort, expected);

      sort.setSort(useDocValues(new SortField("int", SortField.Type.INT)));
      expected = isFull ? "IDHFGJABEC" : "IDHFGJAEBC";
      assertMatches(multi, queryA, sort, expected);
    
      sort.setSort(useDocValues(new SortField ("float", SortField.Type.FLOAT)), SortField.FIELD_DOC);
      assertMatches(multi, queryA, sort, "GDHJCIEFAB");

      sort.setSort(useDocValues(new SortField("float", SortField.Type.FLOAT)));
      assertMatches(multi, queryA, sort, "GDHJCIEFAB");
    
      sort.setSort(useDocValues(new SortField("int", SortField.Type.INT, true)));
      expected = isFull ? "CABEJGFHDI" : "CAEBJGFHDI";
      assertMatches(multi, queryA, sort, expected);
    
      sort.setSort(useDocValues(new SortField("int", SortField.Type.INT)), useDocValues(new SortField("float", SortField.Type.FLOAT)));
      assertMatches(multi, queryA, sort, "IDHFGJEABC");
    
      sort.setSort(useDocValues(new SortField ("int", SortField.Type.INT)));
      assertMatches(multi, queryF, sort, "IZJ");

      sort.setSort(useDocValues(new SortField ("int", SortField.Type.INT, true)));
      assertMatches(multi, queryF, sort, "JZI");
    }
    
    // up to this point, all of the searches should have "sane" 
    // FieldCache behavior, and should have reused hte cache in several cases
    assertSaneFieldCaches(getName() + " various");
    // next we'll check Locale based (String[]) for 'string', so purge first
    FieldCache.DEFAULT.purgeAllCaches();
  }

  private void assertMatches(IndexSearcher searcher, Query query, Sort sort, String expectedResult) throws IOException {
    assertMatches( null, searcher, query, sort, expectedResult );
  }

  // make sure the documents returned by the search match the expected list
  private void assertMatches(String msg, IndexSearcher searcher, Query query, Sort sort,
      String expectedResult) throws IOException {
    //ScoreDoc[] result = searcher.search (query, null, 1000, sort).scoreDocs;
    TopDocs hits = searcher.search (query, null, Math.max(1, expectedResult.length()), sort);
    ScoreDoc[] result = hits.scoreDocs;
    assertEquals(expectedResult.length(),hits.totalHits);
    StringBuilder buff = new StringBuilder(10);
    int n = result.length;
    for (int i=0; i<n; ++i) {
      Document doc = searcher.doc(result[i].doc);
      IndexableField[] v = doc.getFields("tracer");
      for (int j=0; j<v.length; ++j) {
        buff.append (v[j].stringValue());
      }
    }
    assertEquals (msg, expectedResult, buff.toString());
  }

  public void testEmptyStringVsNullStringSort() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(
                        TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    Document doc = new Document();
    doc.add(newField("f", "", StringField.TYPE_UNSTORED));
    doc.add(newField("t", "1", StringField.TYPE_UNSTORED));
    w.addDocument(doc);
    w.commit();
    doc = new Document();
    doc.add(newField("t", "1", StringField.TYPE_UNSTORED));
    w.addDocument(doc);

    IndexReader r = IndexReader.open(w, true);
    w.close();
    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(new TermQuery(new Term("t", "1")), null, 10, new Sort(new SortField("f", SortField.Type.STRING)));
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
        doc.add (new StringField ("string", "a"+i));
        doc.add (new StringField ("string", "b"+i));
        writer.addDocument (doc);
    }
    writer.optimize(); // enforce one segment to have a higher unique term count in all cases
    writer.close();
    sort.setSort(
        new SortField("string", SortField.Type.STRING),
        SortField.FIELD_DOC );
    // this should not throw AIOOBE or RuntimeEx
    IndexSearcher searcher = new IndexSearcher(indexStore, true);
    searcher.search(new MatchAllDocsQuery(), null, 500, sort);
    searcher.close();
    indexStore.close();
  }

  public void testCountingCollector() throws Exception {
    Directory indexStore = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random, indexStore);
    for (int i=0; i<5; i++) {
      Document doc = new Document();
      doc.add (new StringField ("string", "a"+i));
      doc.add (new StringField ("string", "b"+i));
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
}
