package lucli;

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

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import jline.ConsoleReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FieldsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.IndexReader.FieldOption;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.queryParser.MultiFieldQueryParser;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.apache.lucene.util.BytesRef;

/**
 * Various methods that interact with Lucene and provide info about the 
 * index, search, etc. Parts adapted from Lucene demo.
 */
class LuceneMethods {

  private int numDocs;
  private final FSDirectory indexName; //directory of this index
  private List<String> fields; //Fields as a vector
  private List<String> indexedFields; //Fields as a vector
  private String fieldsArray[]; //Fields as an array
  private IndexSearcher searcher;
  private Query query; //current query string
  private String analyzerClassFQN = null; // Analyzer class, if NULL, use default Analyzer

  public LuceneMethods(String index) throws IOException {
    indexName = FSDirectory.open(new File(index));
    message("Lucene CLI. Using directory '" + indexName + "'. Type 'help' for instructions.");
  }

    private Analyzer createAnalyzer() {
        if (analyzerClassFQN == null) return new StandardAnalyzer(Version.LUCENE_CURRENT);
        try {
            return Class.forName(analyzerClassFQN).asSubclass(Analyzer.class).newInstance();
        } catch (ClassCastException cce) {
            message("Given class is not an Analyzer: " + analyzerClassFQN);
            return new StandardAnalyzer(Version.LUCENE_CURRENT);
        } catch (Exception e) {
            message("Unable to use Analyzer " + analyzerClassFQN);
            return new StandardAnalyzer(Version.LUCENE_CURRENT);
        }
    }


  public void info() throws java.io.IOException {
    IndexReader indexReader = IndexReader.open(indexName, true);


    getFieldInfo();
    numDocs = indexReader.numDocs();
    message("Index has " + numDocs + " documents ");
    message("All Fields:" + fields.toString());
    message("Indexed Fields:" + indexedFields.toString());

    if (IndexWriter.isLocked(indexName)) {
      message("Index is locked");
    }
    //IndexReader.getCurrentVersion(indexName);
    //System.out.println("Version:" + version);

    indexReader.close();
  }


  public void search(String queryString, boolean explain, boolean showTokens, ConsoleReader cr)
  		throws java.io.IOException, org.apache.lucene.queryParser.ParseException {
    initSearch(queryString);
    int numHits = computeCount(query);
    message(numHits + " total matching documents");
    if (explain) {
      query = explainQuery(queryString);
    }

    final int HITS_PER_PAGE = 10;
    message("--------------------------------------");
    for (int start = 0; start < numHits; start += HITS_PER_PAGE) {
      int end = Math.min(numHits, start + HITS_PER_PAGE);
      ScoreDoc[] hits = search(query, end);
      for (int ii = start; ii < end; ii++) {
        Document doc = searcher.doc(hits[ii].doc);
        message("---------------- " + (ii + 1) + " score:" + hits[ii].score + "---------------------");
        printHit(doc);
        if (showTokens) {
          invertDocument(doc);
        }
        if (explain) {
          Explanation exp = searcher.explain(query, hits[ii].doc);
          message("Explanation:" + exp.toString());
        }
      }
      message("#################################################");

      if (numHits > end) {
      	// TODO: don't let the input end up in the command line history
      	queryString = cr.readLine("more (y/n) ? ");
        if (queryString.length() == 0 || queryString.charAt(0) == 'n')
          break;
      }
    }
    searcher.close();
  }

  /**
   * TODO: Allow user to specify what field(s) to display
   */
  private void printHit(Document doc) {
    for (int ii = 0; ii < fieldsArray.length; ii++) {
      String currField = fieldsArray[ii];
      String[] result = doc.getValues(currField);
      if (result != null) {
        for (int i = 0; i < result.length; i++) {
          message(currField + ":" + result[i]);
        }
      } else {
        message(currField + ": <not available>");
      }
    }
    //another option is to just do message(doc);
  }

    public void optimize() throws IOException {
    //open the index writer. False: don't create a new one
    IndexWriter indexWriter = new IndexWriter(indexName, new IndexWriterConfig(
        Version.LUCENE_CURRENT, createAnalyzer()).setOpenMode(
        OpenMode.APPEND));
    message("Starting to optimize index.");
    long start = System.currentTimeMillis();
    indexWriter.optimize();
    message("Done optimizing index. Took " + (System.currentTimeMillis() - start) + " msecs");
    indexWriter.close();
  }


    private Query explainQuery(String queryString) throws IOException, ParseException {

    searcher = new IndexSearcher(indexName, true);
    Analyzer analyzer = createAnalyzer();
    getFieldInfo();

    int arraySize = indexedFields.size();
    String indexedArray[] = new String[arraySize];
    for (int ii = 0; ii < arraySize; ii++) {
      indexedArray[ii] = indexedFields.get(ii);
    }
    MultiFieldQueryParser parser = new MultiFieldQueryParser(Version.LUCENE_CURRENT, indexedArray, analyzer);
    query = parser.parse(queryString);
    message("Searching for: " + query.toString());
    return (query);

  }

  /**
   * TODO: Allow user to specify analyzer
   */
  private void initSearch(String queryString) throws IOException, ParseException {

    searcher = new IndexSearcher(indexName, true);
    Analyzer analyzer = createAnalyzer();
    getFieldInfo();

    int arraySize = fields.size();
    fieldsArray = new String[arraySize];
    for (int ii = 0; ii < arraySize; ii++) {
      fieldsArray[ii] = fields.get(ii);
    }
    MultiFieldQueryParser parser = new MultiFieldQueryParser(Version.LUCENE_CURRENT, fieldsArray, analyzer);
    query = parser.parse(queryString);
    System.out.println("Searching for: " + query.toString());
  }
  
  final static class CountingCollector extends Collector {
    public int numHits = 0;
    
    @Override
    public void setScorer(Scorer scorer) throws IOException {}
    @Override
    public void collect(int doc) throws IOException {
      numHits++;
    }

    @Override
    public void setNextReader(AtomicReaderContext context) {}
    @Override
    public boolean acceptsDocsOutOfOrder() {
      return true;
    }    
  }
  
  private int computeCount(Query q) throws IOException {
    CountingCollector countingCollector = new CountingCollector();
    
    searcher.search(q, countingCollector);    
    return countingCollector.numHits;
  }

  public void count(String queryString) throws java.io.IOException, ParseException {
    initSearch(queryString);
    message(computeCount(query) + " total documents");
    searcher.close();
  }
  
  private ScoreDoc[] search(Query q, int numHits) throws IOException {
    return searcher.search(query, numHits).scoreDocs;
  }

  static public void message(String s) {
    System.out.println(s);
  }

  private void getFieldInfo() throws IOException {
    IndexReader indexReader = IndexReader.open(indexName, true);
    fields = new ArrayList<String>();
    indexedFields = new ArrayList<String>();

    //get the list of all field names
    for(String field : indexReader.getFieldNames(FieldOption.ALL)) {
      if (field != null && !field.equals(""))
        fields.add(field.toString());
    }
    //
    //get the list of indexed field names
    for(String field : indexReader.getFieldNames(FieldOption.INDEXED)) {
      if (field != null && !field.equals(""))
        indexedFields.add(field.toString());
    }
    indexReader.close();
  }


  // Copied from DocumentWriter
  // Tokenizes the fields of a document into Postings.
  private void invertDocument(Document doc)
    throws IOException {

    Map<String,Integer> tokenMap = new HashMap<String,Integer>();
    final int maxFieldLength = 10000;

    Analyzer analyzer = createAnalyzer();
    for (Fieldable field : doc.getFields()) {
      String fieldName = field.name();
      if (field.isIndexed()) {
        if (field.isTokenized()) {     // un-tokenized field
          Reader reader;        // find or make Reader
          if (field.readerValue() != null)
            reader = field.readerValue();
          else if (field.stringValue() != null)
            reader = new StringReader(field.stringValue());
          else
            throw new IllegalArgumentException
              ("field must have either String or Reader value");

          int position = 0;
          // Tokenize field and add to postingTable
          TokenStream stream = analyzer.reusableTokenStream(fieldName, reader);
          CharTermAttribute termAtt = stream.addAttribute(CharTermAttribute.class);
          PositionIncrementAttribute posIncrAtt = stream.addAttribute(PositionIncrementAttribute.class);
          
          try {
            stream.reset();
            while (stream.incrementToken()) {
              position += (posIncrAtt.getPositionIncrement() - 1);
              position++;
              String name = termAtt.toString();
              Integer Count = tokenMap.get(name);
              if (Count == null) { // not in there yet
                tokenMap.put(name, Integer.valueOf(1)); //first one
              } else {
                int count = Count.intValue();
                tokenMap.put(name, Integer.valueOf(count + 1));
              }
              if (position > maxFieldLength) break;
            }
            stream.end();
          } finally {
            stream.close();
          }
        }

      }
    }
    Map.Entry<String,Integer>[] sortedHash = getSortedMapEntries(tokenMap);
    for (int ii = 0; ii < sortedHash.length && ii < 10; ii++) {
      Map.Entry<String,Integer> currentEntry = sortedHash[ii];
      message((ii + 1) + ":" + currentEntry.getKey() + " " + currentEntry.getValue());
    }
  }


  /** Provides a list of the top terms of the index.
   *
   * @param field  - the name of the command or null for all of them.
   */
  public void terms(String field) throws IOException {
    TreeMap<String,Integer> termMap = new TreeMap<String,Integer>();
    IndexReader indexReader = IndexReader.open(indexName, true);
    Fields fields = MultiFields.getFields(indexReader);
    if (fields != null) {
      FieldsEnum fieldsEnum = fields.iterator();
      String curField;
      while((curField = fieldsEnum.next()) != null) {
        TermsEnum terms = fieldsEnum.terms();
        BytesRef text;
        while ((text = terms.next()) != null) {
          //message(term.field() + ":" + term.text() + " freq:" + terms.docFreq());
          //if we're either not looking by field or we're matching the specific field
          if ((field == null) || field.equals(curField)) {
            termMap.put(curField + ":" + text.utf8ToString(), Integer.valueOf((terms.docFreq())));
          }
        }
      }
    }

    Iterator<String> termIterator = termMap.keySet().iterator();
    for (int ii = 0; termIterator.hasNext() && ii < 100; ii++) {
      String termDetails = termIterator.next();
      Integer termFreq = termMap.get(termDetails);
      message(termDetails + ": " + termFreq);
    }
    indexReader.close();
  }

  /** Sort Map values
   * @param m the map we're sorting
   * from http://developer.java.sun.com/developer/qow/archive/170/index.jsp
   */
  @SuppressWarnings("unchecked")
  public static <K,V extends Comparable<V>> Map.Entry<K,V>[]
    getSortedMapEntries(Map<K,V> m) {
    Set<Map.Entry<K, V>> set = m.entrySet();
    Map.Entry<K,V>[] entries =
       set.toArray(new Map.Entry[set.size()]);
    Arrays.sort(entries, new Comparator<Map.Entry<K,V>>() {
      public int compare(Map.Entry<K,V> o1, Map.Entry<K,V> o2) {
        V v1 = o1.getValue();
        V v2 = o2.getValue();
        return v2.compareTo(v1); //descending order
      }
    });
    return entries;
  }

    public void analyzer(String word) {
        if ("current".equals(word)) {
            String current = analyzerClassFQN == null ? "StandardAnalyzer" : analyzerClassFQN;
            message("The currently used Analyzer class is: " + current);
            return;
        }
        analyzerClassFQN = word;
        message("Switched to Analyzer class " + analyzerClassFQN);
    }
}

