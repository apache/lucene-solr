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
import java.util.List;
import java.util.ArrayList;
import org.apache.lucene.util.SorterTemplate;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;

/** Implements search over a single IndexReader.
 *
 * <p>Applications usually need only call the inherited {@link #search(Query)}
 * or {@link #search(Query,Filter)} methods. For performance reasons it is 
 * recommended to open only one IndexSearcher and use it for all of your searches.
 * 
 * <p>Note that you can only access Hits from an IndexSearcher as long as it is
 * not yet closed, otherwise an IOException will be thrown. 
 */
public class IndexSearcher extends Searcher {
  IndexReader reader;
  private boolean closeReader;
  private IndexReader[] sortedSubReaders;
  private int[] sortedStarts;

  /** Creates a searcher searching the index in the named directory.
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public IndexSearcher(String path) throws CorruptIndexException, IOException {
    this(IndexReader.open(path), true, false);
  }

  /** Creates a searcher searching the index in the provided directory.
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public IndexSearcher(Directory directory) throws CorruptIndexException, IOException {
    this(IndexReader.open(directory), true, false);
  }

  /** Creates a searcher searching the provided index. */
  public IndexSearcher(IndexReader r) {
    this(r, false, false);
  }
  
  /** Expert: Creates a searcher searching the provided
   *  index, specifying whether searches must visit the
   *  documents in order.  By default, segments are searched
   *  in order of decreasing numDocs(); if you pass true for
   *  docsInOrder, they will instead be searched in their
   *  natural (unsorted) order.*/
  public IndexSearcher(IndexReader r, boolean docsInOrder) {
    this(r, false, docsInOrder);
  }
  
  private IndexSearcher(IndexReader r, boolean closeReader, boolean docsInOrder) {
    reader = r;
    this.closeReader = closeReader;
    sortSubReaders(docsInOrder);
  }

  protected void gatherSubReaders(List allSubReaders, IndexReader r) {
    IndexReader[] subReaders = r.getSequentialSubReaders();
    if (subReaders == null) {
      // Add the reader itself, and do not recurse
      allSubReaders.add(r);
    } else {
      for(int i=0;i<subReaders.length;i++) {
        gatherSubReaders(allSubReaders, subReaders[i]);
      }
    }
  }

  static private final IndexReader[] indexReaderZeroArray = new IndexReader[0];

  protected void sortSubReaders(boolean docsInOrder) {

    List subReadersList = new ArrayList();
    gatherSubReaders(subReadersList, reader);
    sortedSubReaders = (IndexReader[]) subReadersList.toArray(indexReaderZeroArray);
    final int length = sortedSubReaders.length;
    sortedStarts = new int[length];
    int maxDoc = 0;
    for (int i = 0; i < sortedSubReaders.length; i++) {
      sortedStarts[i] = maxDoc;
      maxDoc += sortedSubReaders[i].maxDoc();          // compute maxDocs
    }

    if (!docsInOrder) {

      // sort readers and starts
      SorterTemplate sorter = new SorterTemplate() {
          protected int compare(int i, int j) {
            int num1 = sortedSubReaders[i].numDocs();
            int num2 = sortedSubReaders[j].numDocs();
            if (num1 > num2)
              return -1;
            if (num1 < num2)
              return 1;
            return 0;
          }
          protected void swap(int i, int j) {
            IndexReader temp = sortedSubReaders[i];
            sortedSubReaders[i] = sortedSubReaders[j];
            sortedSubReaders[j] = temp;

            int tempInt = sortedStarts[i];
            sortedStarts[i] = sortedStarts[j];
            sortedStarts[j] = tempInt;
          }
        };
      sorter.quickSort(0, length - 1);
    }
  }
  
  /** Return the {@link IndexReader} this searches. */
  public IndexReader getIndexReader() {
    return reader;
  }

  /**
   * Note that the underlying IndexReader is not closed, if
   * IndexSearcher was constructed with IndexSearcher(IndexReader r).
   * If the IndexReader was supplied implicitly by specifying a directory, then
   * the IndexReader gets closed.
   */
  public void close() throws IOException {
    if(closeReader)
      reader.close();
  }

  // inherit javadoc
  public int docFreq(Term term) throws IOException {
    return reader.docFreq(term);
  }

  // inherit javadoc
  public Document doc(int i) throws CorruptIndexException, IOException {
    return reader.document(i);
  }
  
  // inherit javadoc
  public Document doc(int i, FieldSelector fieldSelector) throws CorruptIndexException, IOException {
	    return reader.document(i, fieldSelector);
  }
  
  // inherit javadoc
  public int maxDoc() throws IOException {
    return reader.maxDoc();
  }

  // inherit javadoc
  public TopDocs search(Weight weight, Filter filter, final int nDocs)
       throws IOException {

    if (nDocs <= 0)  // null might be returned from hq.top() below.
      throw new IllegalArgumentException("nDocs must be > 0");

    TopScoreDocCollector collector = new TopScoreDocCollector(nDocs);
    search(weight, filter, collector);
    return collector.topDocs();
  }

  // inherit javadoc
  public TopFieldDocs search(Weight weight, Filter filter, final int nDocs,
                             Sort sort)
      throws IOException {
    return search(weight, filter, nDocs, sort, true);
  }
  
  /** 
   * Just like {@link #search(Weight, Filter, int, Sort)},
   * but you choose whether or not the fields in the
   * returned {@link FieldDoc} instances should be set by
   * specifying fillFields.
   */
  public TopFieldDocs search(Weight weight, Filter filter, final int nDocs,
                             Sort sort, boolean fillFields)
      throws IOException {
    
    SortField[] fields = sort.fields;
    boolean legacy = false;
    for(int i = 0; i < fields.length; i++) {
      SortField field = fields[i];
      String fieldname = field.getField();
      int type = field.getType();
      // Resolve AUTO into its true type
      if (type == SortField.AUTO) {
        int autotype = FieldValueHitQueue.detectFieldType(reader, fieldname);
        if (autotype == SortField.STRING) {
          fields[i] = new SortField (fieldname, field.getLocale(), field.getReverse());
        } else {
          fields[i] = new SortField (fieldname, autotype, field.getReverse());
        }
      }

      if (field.getUseLegacySearch()) {
        legacy = true;
      }
    }
    
    if (legacy) {
      // Search the single top-level reader
      TopScoreDocCollector collector = new TopFieldDocCollector(reader, sort, nDocs);
      collector.setNextReader(reader, 0);
      doSearch(reader, weight, filter, collector);
      return (TopFieldDocs) collector.topDocs();
    } else {
      // Search each sub-reader
      TopFieldCollector collector = new TopFieldCollector(sort, nDocs, sortedSubReaders, fillFields);
      search(weight, filter, collector);
      return (TopFieldDocs) collector.topDocs();
    }
  }

  // inherit javadoc
  public void search(Weight weight, Filter filter, HitCollector results)
      throws IOException {

    final MultiReaderHitCollector collector;
    if (results instanceof MultiReaderHitCollector) {
      collector = (MultiReaderHitCollector) results;
    } else {
      collector = new MultiReaderCollectorWrapper(results);
    }

    for (int i = 0; i < sortedSubReaders.length; i++) { // search each subreader
      collector.setNextReader(sortedSubReaders[i], sortedStarts[i]);
      doSearch(sortedSubReaders[i], weight, filter, collector);
    }
  }
  
  private void doSearch(IndexReader reader, Weight weight, Filter filter,
      final HitCollector results) throws IOException {

    Scorer scorer = weight.scorer(reader);
    if (scorer == null)
      return;

    if (filter == null) {
      scorer.score(results);
      return;
    }

    DocIdSetIterator filterDocIdIterator = filter.getDocIdSet(reader).iterator(); // CHECKME: use ConjunctionScorer here?
    
    boolean more = filterDocIdIterator.next() && scorer.skipTo(filterDocIdIterator.doc());

    while (more) {
      int filterDocId = filterDocIdIterator.doc();
      if (filterDocId > scorer.doc() && !scorer.skipTo(filterDocId)) {
        more = false;
      } else {
        int scorerDocId = scorer.doc();
        if (scorerDocId == filterDocId) { // permitted by filter
          results.collect(scorerDocId, scorer.score());
          more = filterDocIdIterator.next();
        } else {
          more = filterDocIdIterator.skipTo(scorerDocId);
        }
      }
    }
  }

  public Query rewrite(Query original) throws IOException {
    Query query = original;
    for (Query rewrittenQuery = query.rewrite(reader); rewrittenQuery != query;
         rewrittenQuery = query.rewrite(reader)) {
      query = rewrittenQuery;
    }
    return query;
  }

  public Explanation explain(Weight weight, int doc) throws IOException {
    return weight.explain(reader, doc);
  }
  
  /**
   * Wrapper for non expert ({@link HitCollector})
   * implementations, which simply re-bases the incoming
   * docID before calling {@link HitCollector#collect}.
   */
  static class MultiReaderCollectorWrapper extends MultiReaderHitCollector {
    private HitCollector collector;
    private int base = -1;

    public MultiReaderCollectorWrapper(HitCollector collector) {
      this.collector = collector;
    }
    
    public void collect(int doc, float score) {
      collector.collect(doc + base, score);
    }

    public void setNextReader(IndexReader reader, int docBase) {
      base = docBase;
    }
  }
}
