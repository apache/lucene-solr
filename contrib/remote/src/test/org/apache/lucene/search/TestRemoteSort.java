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
import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Random;

import junit.framework.Test;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

/**
 * Unit tests for remote sorting code.
 * Note: This is a modified copy of {@link TestSort} without duplicated test
 * methods and therefore unused members and methodes. 
 *
 * @version $Id:$
 */

public class TestRemoteSort extends LuceneTestCase implements Serializable {

  private Searcher full;
  private Query queryX;
  private Query queryY;
  private Query queryA;
  private Query queryF;
  private Sort sort;


  public TestRemoteSort (String name) {
    super (name);
  }

  public static void main (String[] argv) {
    if (argv == null || argv.length < 1)
      TestRunner.run (suite());
    else if ("server".equals (argv[0])) {
      TestRemoteSort test = new TestRemoteSort (null);
      try {
        test.startServer();
        Thread.sleep (500000);
      } catch (Exception e) {
        System.out.println (e);
        e.printStackTrace();
      }
    }
  }

  public static Test suite() {
    return new TestSuite (TestRemoteSort.class);
  }


  // document data:
  // the tracer field is used to determine which document was hit
  // the contents field is used to search and sort by relevance
  // the int field to sort by int
  // the float field to sort by float
  // the string field to sort by string
    // the i18n field includes accented characters for testing locale-specific sorting
  private String[][] data = new String[][] {
  // tracer  contents         int            float           string   custom   i18n               long            double, 'short', byte, 'custom parser encoding'
  {   "A",   "x a",           "5",           "4f",           "c",     "A-3",   "p\u00EAche",      "10",           "-4.0", "3", "126", "J"},//A, x
  {   "B",   "y a",           "5",           "3.4028235E38", "i",     "B-10",  "HAT",             "1000000000", "40.0", "24", "1", "I"},//B, y
  {   "C",   "x a b c",       "2147483647",  "1.0",          "j",     "A-2",   "p\u00E9ch\u00E9", "99999999",   "40.00002343", "125", "15", "H"},//C, x
  {   "D",   "y a b c",       "-1",          "0.0f",         "a",     "C-0",   "HUT",             String.valueOf(Long.MAX_VALUE),           String.valueOf(Double.MIN_VALUE), String.valueOf(Short.MIN_VALUE), String.valueOf(Byte.MIN_VALUE), "G"},//D, y
  {   "E",   "x a b c d",     "5",           "2f",           "h",     "B-8",   "peach",           String.valueOf(Long.MIN_VALUE),           String.valueOf(Double.MAX_VALUE), String.valueOf(Short.MAX_VALUE),           String.valueOf(Byte.MAX_VALUE), "F"},//E,x
  {   "F",   "y a b c d",     "2",           "3.14159f",     "g",     "B-1",   "H\u00C5T",        "-44",           "343.034435444", "-3", "0", "E"},//F,y
  {   "G",   "x a b c d",     "3",           "-1.0",         "f",     "C-100", "sin",             "323254543543", "4.043544", "5", "100", "D"},//G,x
  {   "H",   "y a b c d",     "0",           "1.4E-45",      "e",     "C-88",  "H\u00D8T",        "1023423423005","4.043545", "10", "-50", "C"},//H,y
  {   "I",   "x a b c d e f", "-2147483648", "1.0e+0",       "d",     "A-10",  "s\u00EDn",        "332422459999", "4.043546", "-340", "51", "B"},//I,x
  {   "J",   "y a b c d e f", "4",           ".5",           "b",     "C-7",   "HOT",             "34334543543",  "4.0000220343", "300", "2", "A"},//J,y
  {   "W",   "g",             "1",           null,           null,    null,    null,              null,           null, null, null, null},
  {   "X",   "g",             "1",           "0.1",          null,    null,    null,              null,           null, null, null, null},
  {   "Y",   "g",             "1",           "0.2",          null,    null,    null,              null,           null, null, null, null},
  {   "Z",   "f g",           null,          null,           null,    null,    null,              null,           null, null, null, null}
  };
  
  // create an index of all the documents, or just the x, or just the y documents
  private Searcher getIndex (boolean even, boolean odd)
  throws IOException {
    RAMDirectory indexStore = new RAMDirectory ();
    IndexWriter writer = new IndexWriter (indexStore, new SimpleAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
    writer.setMaxBufferedDocs(2);
    writer.setMergeFactor(1000);
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
    //writer.optimize ();
    writer.close ();
    IndexSearcher s = new IndexSearcher (indexStore, false);
    s.setDefaultFieldSortScoring(true, true);
    return s;
  }

  private Searcher getFullIndex()
  throws IOException {
    return getIndex (true, true);
  }
  
  public String getRandomNumberString(int num, int low, int high) {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < num; i++) {
      sb.append(getRandomNumber(low, high));
    }
    return sb.toString();
  }
  
  public String getRandomCharString(int num) {
    return getRandomCharString(num, 48, 122);
  }
  
  public String getRandomCharString(int num, int start, int end) {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < num; i++) {
      sb.append(new Character((char) getRandomNumber(start, end)));
    }
    return sb.toString();
  }
  
  Random r;
  
  public int getRandomNumber(final int low, final int high) {
  
    int randInt = (Math.abs(r.nextInt()) % (high - low)) + low;

    return randInt;
  }

  public void setUp() throws Exception {
    super.setUp();
    full = getFullIndex();
    queryX = new TermQuery (new Term ("contents", "x"));
    queryY = new TermQuery (new Term ("contents", "y"));
    queryA = new TermQuery (new Term ("contents", "a"));
    queryF = new TermQuery (new Term ("contents", "f"));
    sort = new Sort();
  }


  static class MyFieldComparator extends FieldComparator {
    int[] docValues;
    int[] slotValues;
    int bottomValue;

    MyFieldComparator(int numHits) {
      slotValues = new int[numHits];
    }

    public void copy(int slot, int doc) {
      slotValues[slot] = docValues[doc];
    }

    public int compare(int slot1, int slot2) {
      return slotValues[slot1] - slotValues[slot2];
    }

    public int compareBottom(int doc) {
      return bottomValue - docValues[doc];
    }

    public void setBottom(int bottom) {
      bottomValue = slotValues[bottom];
    }

    public void setNextReader(IndexReader reader, int docBase) throws IOException {
      docValues = FieldCache.DEFAULT.getInts(reader, "parser", new FieldCache.IntParser() {
          public final int parseInt(final String val) {
            return (val.charAt(0)-'A') * 123456;
          }
        });
    }

    public Comparable value(int slot) {
      return new Integer(slotValues[slot]);
    }
  }

  static class MyFieldComparatorSource extends FieldComparatorSource {
    public FieldComparator newComparator(String fieldname, int numHits, int sortPos, boolean reversed) {
      return new MyFieldComparator(numHits);
    }
  }

  // test a variety of sorts using a remote searcher
  public void testRemoteSort() throws Exception {
    Searchable searcher = getRemote();
    MultiSearcher multi = new MultiSearcher (new Searchable[] { searcher });
    runMultiSorts(multi, true); // this runs on the full index
  }

  // test custom search when remote
  public void testRemoteCustomSort() throws Exception {
    Searchable searcher = getRemote();
    MultiSearcher multi = new MultiSearcher (new Searchable[] { searcher });
    sort.setSort (new SortField ("custom", SampleComparable.getComparatorSource()));
    assertMatches (multi, queryX, sort, "CAIEG");
    sort.setSort (new SortField ("custom", SampleComparable.getComparatorSource(), true));
    assertMatches (multi, queryY, sort, "HJDBF");

    assertSaneFieldCaches(getName() + " ComparatorSource");
    FieldCache.DEFAULT.purgeAllCaches();

    SortComparator custom = SampleComparable.getComparator();
    sort.setSort (new SortField ("custom", custom));
    assertMatches (multi, queryX, sort, "CAIEG");
    sort.setSort (new SortField ("custom", custom, true));
    assertMatches (multi, queryY, sort, "HJDBF");

    assertSaneFieldCaches(getName() + " Comparator");
    FieldCache.DEFAULT.purgeAllCaches();
  }

  // test that the relevancy scores are the same even if
  // hits are sorted
  public void testNormalizedScores() throws Exception {

    // capture relevancy scores
    HashMap scoresX = getScores (full.search (queryX, null, 1000).scoreDocs, full);
    HashMap scoresY = getScores (full.search (queryY, null, 1000).scoreDocs, full);
    HashMap scoresA = getScores (full.search (queryA, null, 1000).scoreDocs, full);

    // we'll test searching locally, remote and multi
    MultiSearcher remote = new MultiSearcher (new Searchable[] { getRemote() });

    // change sorting and make sure relevancy stays the same

    sort = new Sort();
    assertSameValues (scoresX, getScores (remote.search (queryX, null, 1000, sort).scoreDocs, remote));
    assertSameValues (scoresY, getScores (remote.search (queryY, null, 1000, sort).scoreDocs, remote));
    assertSameValues (scoresA, getScores (remote.search (queryA, null, 1000, sort).scoreDocs, remote));

    sort.setSort(SortField.FIELD_DOC);
    assertSameValues (scoresX, getScores (remote.search (queryX, null, 1000, sort).scoreDocs, remote));
    assertSameValues (scoresY, getScores (remote.search (queryY, null, 1000, sort).scoreDocs, remote));
    assertSameValues (scoresA, getScores (remote.search (queryA, null, 1000, sort).scoreDocs, remote));

    sort.setSort ("int");
    assertSameValues (scoresX, getScores (remote.search (queryX, null, 1000, sort).scoreDocs, remote));
    assertSameValues (scoresY, getScores (remote.search (queryY, null, 1000, sort).scoreDocs, remote));
    assertSameValues (scoresA, getScores (remote.search (queryA, null, 1000, sort).scoreDocs, remote));

    sort.setSort ("float");
    assertSameValues (scoresX, getScores (remote.search (queryX, null, 1000, sort).scoreDocs, remote));
    assertSameValues (scoresY, getScores (remote.search (queryY, null, 1000, sort).scoreDocs, remote));
    assertSameValues (scoresA, getScores (remote.search (queryA, null, 1000, sort).scoreDocs, remote));

    sort.setSort ("string");
    assertSameValues (scoresX, getScores (remote.search (queryX, null, 1000, sort).scoreDocs, remote));
    assertSameValues (scoresY, getScores (remote.search (queryY, null, 1000, sort).scoreDocs, remote));
    assertSameValues (scoresA, getScores (remote.search (queryA, null, 1000, sort).scoreDocs, remote));

    sort.setSort (new String[] {"int","float"});
    assertSameValues (scoresX, getScores (remote.search (queryX, null, 1000, sort).scoreDocs, remote));
    assertSameValues (scoresY, getScores (remote.search (queryY, null, 1000, sort).scoreDocs, remote));
    assertSameValues (scoresA, getScores (remote.search (queryA, null, 1000, sort).scoreDocs, remote));

    sort.setSort (new SortField[] { new SortField ("int", SortField.INT, true), new SortField (null, SortField.DOC, true) });
    assertSameValues (scoresX, getScores (remote.search (queryX, null, 1000, sort).scoreDocs, remote));
    assertSameValues (scoresY, getScores (remote.search (queryY, null, 1000, sort).scoreDocs, remote));
    assertSameValues (scoresA, getScores (remote.search (queryA, null, 1000, sort).scoreDocs, remote));

    sort.setSort (new String[] {"float","string"});
    assertSameValues (scoresX, getScores (remote.search (queryX, null, 1000, sort).scoreDocs, remote));
    assertSameValues (scoresY, getScores (remote.search (queryY, null, 1000, sort).scoreDocs, remote));
    assertSameValues (scoresA, getScores (remote.search (queryA, null, 1000, sort).scoreDocs, remote));
  }
  
  // runs a variety of sorts useful for multisearchers
  private void runMultiSorts(Searcher multi, boolean isFull) throws Exception {
    sort.setSort(SortField.FIELD_DOC);
    String expected = isFull ? "ABCDEFGHIJ" : "ACEGIBDFHJ";
    assertMatches(multi, queryA, sort, expected);

    sort.setSort(new SortField ("int", SortField.INT));
    expected = isFull ? "IDHFGJABEC" : "IDHFGJAEBC";
    assertMatches(multi, queryA, sort, expected);

    sort.setSort(new SortField[] {new SortField ("int", SortField.INT), SortField.FIELD_DOC});
    expected = isFull ? "IDHFGJABEC" : "IDHFGJAEBC";
    assertMatches(multi, queryA, sort, expected);

    sort.setSort("int");
    expected = isFull ? "IDHFGJABEC" : "IDHFGJAEBC";
    assertMatches(multi, queryA, sort, expected);

    sort.setSort(new SortField[] {new SortField ("float", SortField.FLOAT), SortField.FIELD_DOC});
    assertMatches(multi, queryA, sort, "GDHJCIEFAB");

    sort.setSort("float");
    assertMatches(multi, queryA, sort, "GDHJCIEFAB");

    sort.setSort("string");
    assertMatches(multi, queryA, sort, "DJAIHGFEBC");

    sort.setSort("int", true);
    expected = isFull ? "CABEJGFHDI" : "CAEBJGFHDI";
    assertMatches(multi, queryA, sort, expected);

    sort.setSort("float", true);
    assertMatches(multi, queryA, sort, "BAFECIJHDG");

    sort.setSort("string", true);
    assertMatches(multi, queryA, sort, "CBEFGHIAJD");

    sort.setSort(new String[] {"int","float"});
    assertMatches(multi, queryA, sort, "IDHFGJEABC");

    sort.setSort(new String[] {"float","string"});
    assertMatches(multi, queryA, sort, "GDHJICEFAB");

    sort.setSort("int");
    assertMatches(multi, queryF, sort, "IZJ");

    sort.setSort("int", true);
    assertMatches(multi, queryF, sort, "JZI");

    sort.setSort("float");
    assertMatches(multi, queryF, sort, "ZJI");

    sort.setSort("string");
    assertMatches(multi, queryF, sort, "ZJI");

    sort.setSort("string", true);
    assertMatches(multi, queryF, sort, "IJZ");

    // up to this point, all of the searches should have "sane" 
    // FieldCache behavior, and should have reused hte cache in several cases
    assertSaneFieldCaches(getName() + " Basics");
    // next we'll check an alternate Locale for string, so purge first
    FieldCache.DEFAULT.purgeAllCaches();

    sort.setSort(new SortField[] { new SortField ("string", Locale.US) });
    assertMatches(multi, queryA, sort, "DJAIHGFEBC");

    sort.setSort(new SortField[] { new SortField ("string", Locale.US, true)});
    assertMatches(multi, queryA, sort, "CBEFGHIAJD");

    assertSaneFieldCaches(getName() + " Locale.US");
    FieldCache.DEFAULT.purgeAllCaches();
  }

  // make sure the documents returned by the search match the expected list
  private void assertMatches(Searcher searcher, Query query, Sort sort,
      String expectedResult) throws IOException {
    //ScoreDoc[] result = searcher.search (query, null, 1000, sort).scoreDocs;
    TopDocs hits = searcher.search (query, null, expectedResult.length(), sort);
    ScoreDoc[] result = hits.scoreDocs;
    assertEquals(hits.totalHits, expectedResult.length());
    StringBuffer buff = new StringBuffer(10);
    int n = result.length;
    for (int i=0; i<n; ++i) {
      Document doc = searcher.doc(result[i].doc);
      String[] v = doc.getValues("tracer");
      for (int j=0; j<v.length; ++j) {
        buff.append (v[j]);
      }
    }
    assertEquals (expectedResult, buff.toString());
  }

  private HashMap getScores (ScoreDoc[] hits, Searcher searcher)
  throws IOException {
    HashMap scoreMap = new HashMap();
    int n = hits.length;
    for (int i=0; i<n; ++i) {
      Document doc = searcher.doc(hits[i].doc);
      String[] v = doc.getValues("tracer");
      assertEquals (v.length, 1);
      scoreMap.put (v[0], new Float(hits[i].score));
    }
    return scoreMap;
  }

  // make sure all the values in the maps match
  private void assertSameValues (HashMap m1, HashMap m2) {
    int n = m1.size();
    int m = m2.size();
    assertEquals (n, m);
    Iterator iter = m1.keySet().iterator();
    while (iter.hasNext()) {
      Object key = iter.next();
      Object o1 = m1.get(key);
      Object o2 = m2.get(key);
      if (o1 instanceof Float) {
        assertEquals(((Float)o1).floatValue(), ((Float)o2).floatValue(), 1e-6);
      } else {
        assertEquals (m1.get(key), m2.get(key));
      }
    }
  }

  private Searchable getRemote () throws Exception {
    try {
      return lookupRemote ();
    } catch (Throwable e) {
      startServer ();
      return lookupRemote ();
    }
  }

  private Searchable lookupRemote () throws Exception {
    return (Searchable) Naming.lookup ("//localhost:" + port + "/SortedSearchable");
  }

  private int port = -1;

  private void startServer () throws Exception {
    // construct an index
    port = _TestUtil.getRandomSocketPort();
    Searcher local = getFullIndex();
    // local.search (queryA, new Sort());

    // publish it
    LocateRegistry.createRegistry (port);
    RemoteSearchable impl = new RemoteSearchable (local);
    Naming.rebind ("//localhost:" + port + "/SortedSearchable", impl);
  }

}
