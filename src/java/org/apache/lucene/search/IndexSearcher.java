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
import java.util.List;

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
  private IndexReader[] subReaders;
  private int[] docStarts;

  /** Creates a searcher searching the index in the named directory.
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   * @deprecated Use {@link #IndexSearcher(String, boolean)} instead
   */
  public IndexSearcher(String path) throws CorruptIndexException, IOException {
    this(IndexReader.open(path), true);
  }

  /** Creates a searcher searching the index in the named
   *  directory.  You should pass readOnly=true, since it
   *  gives much better concurrent performance, unless you
   *  intend to do write operations (delete documents or
   *  change norms) with the underlying IndexReader.
   * @param path directory where IndexReader will be opened
   * @param readOnly if true, the underlying IndexReader
   * will be opened readOnly
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public IndexSearcher(String path, boolean readOnly) throws CorruptIndexException, IOException {
    this(IndexReader.open(path, readOnly), true);
  }

  /** Creates a searcher searching the index in the provided directory.
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   * @deprecated Use {@link #IndexSearcher(Directory, boolean)} instead
   */
  public IndexSearcher(Directory directory) throws CorruptIndexException, IOException {
    this(IndexReader.open(directory), true);
  }

  /** Creates a searcher searching the index in the named
   *  directory.  You should pass readOnly=true, since it
   *  gives much better concurrent performance, unless you
   *  intend to do write operations (delete documents or
   *  change norms) with the underlying IndexReader.
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   * @param path directory where IndexReader will be opened
   * @param readOnly if true, the underlying IndexReader
   * will be opened readOnly
   */
  public IndexSearcher(Directory path, boolean readOnly) throws CorruptIndexException, IOException {
    this(IndexReader.open(path, readOnly), true);
  }

  /** Creates a searcher searching the provided index. */
  public IndexSearcher(IndexReader r) {
    this(r, false);
  }
  
  private IndexSearcher(IndexReader r, boolean closeReader) {
    reader = r;
    this.closeReader = closeReader;

    List subReadersList = new ArrayList();
    gatherSubReaders(subReadersList, reader);
    subReaders = (IndexReader[]) subReadersList.toArray(new IndexReader[subReadersList.size()]);
    docStarts = new int[subReaders.length];
    int maxDoc = 0;
    for (int i = 0; i < subReaders.length; i++) {
      docStarts[i] = maxDoc;
      maxDoc += subReaders[i].maxDoc();
    }
  }

  protected void gatherSubReaders(List allSubReaders, IndexReader r) {
    IndexReader[] subReaders = r.getSequentialSubReaders();
    if (subReaders == null) {
      // Add the reader itself, and do not recurse
      allSubReaders.add(r);
    } else {
      for (int i = 0; i < subReaders.length; i++) {
        gatherSubReaders(allSubReaders, subReaders[i]);
      }
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

    // TODO: The following should be changed to first obtain a Scorer and then ask it
    // if it's going to return in-order or out-of-order docs, and create TSDC
    // accordingly.
    TopScoreDocCollector collector = TopScoreDocCollector.create(nDocs, false);
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
   * Just like {@link #search(Weight, Filter, int, Sort)}, but you choose
   * whether or not the fields in the returned {@link FieldDoc} instances should
   * be set by specifying fillFields.<br>
   * <b>NOTE:</b> currently, this method tracks document scores and sets them in
   * the returned {@link FieldDoc}, however in 3.0 it will move to not track
   * document scores. If document scores tracking is still needed, you can use
   * {@link #search(Weight, Filter, Collector)} and pass in a
   * {@link TopFieldCollector} instance.
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
        int autotype = SortField.detectFieldType(reader, fieldname);
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
      TopDocCollector collector = new TopFieldDocCollector(reader, sort, nDocs);
      HitCollectorWrapper hcw = new HitCollectorWrapper(collector);
      hcw.setNextReader(reader, 0);
      doSearch(reader, weight, filter, hcw);
      return (TopFieldDocs) collector.topDocs();
    }
    // Search each sub-reader
    // TODO: The following should be changed to first obtain a Scorer and then ask it
    // if it's going to return in-order or out-of-order docs, and create TSDC
    // accordingly.
    TopFieldCollector collector = TopFieldCollector.create(sort, nDocs,
        fillFields, fieldSortDoTrackScores, fieldSortDoMaxScore, false);
    search(weight, filter, collector);
    return (TopFieldDocs) collector.topDocs();
  }

  // inherit javadoc
  /** @deprecated use {@link #search(Weight, Filter, Collector)} instead. */
  public void search(Weight weight, Filter filter, HitCollector results)
      throws IOException {
    search(weight, filter, new HitCollectorWrapper(results));
  }
  
  // inherit javadoc
  public void search(Weight weight, Filter filter, Collector collector)
      throws IOException {
    
    for (int i = 0; i < subReaders.length; i++) { // search each subreader
      collector.setNextReader(subReaders[i], docStarts[i]);
      doSearch(subReaders[i], weight, filter, collector);
    }
  }
  
  private void doSearch(IndexReader reader, Weight weight, Filter filter,
      final Collector collector) throws IOException {

    Scorer scorer = weight.scorer(reader);
    if (scorer == null)
      return;

    if (filter == null) {
      scorer.score(collector);
      return;
    }

    DocIdSetIterator filterDocIdIterator = filter.getDocIdSet(reader).iterator(); // CHECKME: use ConjunctionScorer here?
    
    boolean more = filterDocIdIterator.next() && scorer.skipTo(filterDocIdIterator.doc());

    collector.setScorer(scorer);
    while (more) {
      int filterDocId = filterDocIdIterator.doc();
      if (filterDocId > scorer.doc() && !scorer.skipTo(filterDocId)) {
        more = false;
      } else {
        int scorerDocId = scorer.doc();
        if (scorerDocId == filterDocId) { // permitted by filter
          collector.collect(scorerDocId);
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

  private boolean fieldSortDoTrackScores;
  private boolean fieldSortDoMaxScore;

  /** @deprecated */
  public void setDefaultFieldSortScoring(boolean doTrackScores, boolean doMaxScore) {
    fieldSortDoTrackScores = doTrackScores;
    fieldSortDoMaxScore = doMaxScore;
  }
}
