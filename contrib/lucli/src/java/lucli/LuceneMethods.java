package lucli;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;

import jline.ConsoleReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.IndexReader.FieldOption;
import org.apache.lucene.queryParser.MultiFieldQueryParser;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Searcher;

/**
 * Various methods that interact with Lucene and provide info about the 
 * index, search, etc. Parts adapted from Lucene demo.
 */
class LuceneMethods {

  private int numDocs;
  private String indexName; //directory of this index
  private java.util.Iterator fieldIterator;
  private List fields; //Fields as a vector
  private List indexedFields; //Fields as a vector
  private String fieldsArray[]; //Fields as an array
  private Searcher searcher;
  private Query query; //current query string

  public LuceneMethods(String index) {
    indexName = index;
    message("Lucene CLI. Using directory '" + indexName + "'. Type 'help' for instructions.");
  }


  public void info() throws java.io.IOException {
    IndexReader indexReader = IndexReader.open(indexName);


    getFieldInfo();
    numDocs = indexReader.numDocs();
    message("Index has " + numDocs + " documents ");
    message("All Fields:" + fields.toString());
    message("Indexed Fields:" + indexedFields.toString());

    if (IndexReader.isLocked(indexName)) {
      message("Index is locked");
    }
    //IndexReader.getCurrentVersion(indexName);
    //System.out.println("Version:" + version);

    indexReader.close();
  }


  public void search(String queryString, boolean explain, boolean showTokens, ConsoleReader cr)
  		throws java.io.IOException, org.apache.lucene.queryParser.ParseException {
    Hits hits = initSearch(queryString);
    System.out.println(hits.length() + " total matching documents");
    if (explain) {
      query = explainQuery(queryString);
    }

    final int HITS_PER_PAGE = 10;
    message("--------------------------------------");
    for (int start = 0; start < hits.length(); start += HITS_PER_PAGE) {
      int end = Math.min(hits.length(), start + HITS_PER_PAGE);
      for (int ii = start; ii < end; ii++) {
        Document doc = hits.doc(ii);
        message("---------------- " + (ii + 1) + " score:" + hits.score(ii) + "---------------------");
        printHit(doc);
        if (showTokens) {
          invertDocument(doc);
        }
        if (explain) {
          Explanation exp = searcher.explain(query, hits.id(ii));
          message("Explanation:" + exp.toString());
        }
      }
      message("#################################################");

      if (hits.length() > end) {
      	// TODO: don't let the input end up in the command line history
      	queryString = cr.readLine("more (y/n) ? ");
        if (queryString.length() == 0 || queryString.charAt(0) == 'n')
          break;
      }
    }
    searcher.close();
  }

  /**
   * @todo Allow user to specify what field(s) to display
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
    IndexWriter indexWriter = new IndexWriter(indexName, new StandardAnalyzer(), false);
    message("Starting to optimize index.");
    long start = System.currentTimeMillis();
    indexWriter.optimize();
    message("Done optimizing index. Took " + (System.currentTimeMillis() - start) + " msecs");
    indexWriter.close();
  }


  private Query explainQuery(String queryString) throws IOException, ParseException {

    searcher = new IndexSearcher(indexName);
    Analyzer analyzer = new StandardAnalyzer();
    getFieldInfo();

    int arraySize = indexedFields.size();
    String indexedArray[] = new String[arraySize];
    for (int ii = 0; ii < arraySize; ii++) {
      indexedArray[ii] = (String) indexedFields.get(ii);
    }
    MultiFieldQueryParser parser = new MultiFieldQueryParser(indexedArray, analyzer);
    query = parser.parse(queryString);
    System.out.println("Searching for: " + query.toString());
    return (query);

  }

  /**
   * @todo Allow user to specify analyzer
   */
  private Hits initSearch(String queryString) throws IOException, ParseException {

    searcher = new IndexSearcher(indexName);
    Analyzer analyzer = new StandardAnalyzer();
    getFieldInfo();

    int arraySize = fields.size();
    fieldsArray = new String[arraySize];
    for (int ii = 0; ii < arraySize; ii++) {
      fieldsArray[ii] = (String) fields.get(ii);
    }
    MultiFieldQueryParser parser = new MultiFieldQueryParser(fieldsArray, analyzer);
    query = parser.parse(queryString);
    System.out.println("Searching for: " + query.toString());
    Hits hits = searcher.search(query);
    return (hits);

  }

  public void count(String queryString) throws java.io.IOException, ParseException {
    Hits hits = initSearch(queryString);
    System.out.println(hits.length() + " total documents");
    searcher.close();
  }

  static public void message(String s) {
    System.out.println(s);
  }

  private void getFieldInfo() throws IOException {
    IndexReader indexReader = IndexReader.open(indexName);
    fields = new ArrayList();
    indexedFields = new ArrayList();

    //get the list of all field names
    fieldIterator = indexReader.getFieldNames(FieldOption.ALL).iterator();
    while (fieldIterator.hasNext()) {
      Object field = fieldIterator.next();
      if (field != null && !field.equals(""))
        fields.add(field.toString());
    }
    //
    //get the list of indexed field names
    fieldIterator = indexReader.getFieldNames(FieldOption.INDEXED).iterator();
    while (fieldIterator.hasNext()) {
      Object field = fieldIterator.next();
      if (field != null && !field.equals(""))
        indexedFields.add(field.toString());
    }
    indexReader.close();
  }


  // Copied from DocumentWriter
  // Tokenizes the fields of a document into Postings.
  private void invertDocument(Document doc)
    throws IOException {

    Map tokenMap = new HashMap();
    final int maxFieldLength = 10000;

    Analyzer analyzer = new StandardAnalyzer();
    Iterator fields = doc.getFields().iterator();
    final Token reusableToken = new Token();
    while (fields.hasNext()) {
      Field field = (Field) fields.next();
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
          TokenStream stream = analyzer.tokenStream(fieldName, reader);
          try {
            for (Token nextToken = stream.next(reusableToken); nextToken != null; nextToken = stream.next(reusableToken)) {
              position += (nextToken.getPositionIncrement() - 1);
              position++;
              String name = nextToken.term();
              Integer Count = (Integer) tokenMap.get(name);
              if (Count == null) { // not in there yet
                tokenMap.put(name, new Integer(1)); //first one
              } else {
                int count = Count.intValue();
                tokenMap.put(name, new Integer(count + 1));
              }
              if (position > maxFieldLength) break;
            }
          } finally {
            stream.close();
          }
        }

      }
    }
    Entry[] sortedHash = getSortedMapEntries(tokenMap);
    for (int ii = 0; ii < sortedHash.length && ii < 10; ii++) {
      Entry currentEntry = sortedHash[ii];
      message((ii + 1) + ":" + currentEntry.getKey() + " " + currentEntry.getValue());
    }
  }


  /** Provides a list of the top terms of the index.
   *
   * @param field  - the name of the command or null for all of them.
   */
  public void terms(String field) throws IOException {
    TreeMap termMap = new TreeMap();
    IndexReader indexReader = IndexReader.open(indexName);
    TermEnum terms = indexReader.terms();
    while (terms.next()) {
      Term term = terms.term();
      //message(term.field() + ":" + term.text() + " freq:" + terms.docFreq());
      //if we're either not looking by field or we're matching the specific field
      if ((field == null) || field.equals(term.field()))
        termMap.put(term.field() + ":" + term.text(), new Integer((terms.docFreq())));
    }

    Iterator termIterator = termMap.keySet().iterator();
    for (int ii = 0; termIterator.hasNext() && ii < 100; ii++) {
      String termDetails = (String) termIterator.next();
      Integer termFreq = (Integer) termMap.get(termDetails);
      message(termDetails + ": " + termFreq);
    }
    indexReader.close();
  }

  /** Sort Map values
   * @param m the map we're sorting
   * from http://developer.java.sun.com/developer/qow/archive/170/index.jsp
   */
  public static Entry[]
    getSortedMapEntries(Map m) {
    Set set = m.entrySet();
    Entry[] entries =
      (Entry[]) set.toArray(
          new Entry[set.size()]);
    Arrays.sort(entries, new Comparator() {
      public int compare(Object o1, Object o2) {
        Object v1 = ((Entry) o1).getValue();
        Object v2 = ((Entry) o2).getValue();
        return ((Comparable) v2).compareTo(v1); //descending order
      }
    });
    return entries;
  }

}

