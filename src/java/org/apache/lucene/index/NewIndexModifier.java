package org.apache.lucene.index;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.store.Directory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

/**
 * NewIndexModifier extends {@link IndexWriter} so that you can not only insert
 * documents but also delete documents through a single interface. Internally,
 * inserts and deletes are buffered before they are flushed to disk.
 * <p>
 * Design Overview
 * <p>
 * deleteDocuments() method works by buffering terms to be deleted. Deletes are
 * deferred until ram is flushed to disk, either because enough new documents or
 * delete terms are buffered, or because close() or flush() is called. Using
 * Java synchronization, care is taken to ensure that an interleaved sequence of
 * inserts and deletes for the same document are properly serialized.
 */

public class NewIndexModifier extends IndexWriter {
  // number of ram segments a delete term applies to
  private class Num {
    private int num;

    Num(int num) {
      this.num = num;
    }

    int getNum() {
      return num;
    }

    void setNum(int num) {
      this.num = num;
    }
  }

  /**
   * Default value is 10. Change using {@link #setMaxBufferedDeleteTerms(int)}.
   */
  public final static int DEFAULT_MAX_BUFFERED_DELETE_TERMS = 10;
  // the max number of delete terms that can be buffered before
  // they must be flushed to disk
  private int maxBufferedDeleteTerms = DEFAULT_MAX_BUFFERED_DELETE_TERMS;

  // to buffer delete terms in ram before they are applied
  // key is delete term, value is number of ram segments the term applies to
  private HashMap bufferedDeleteTerms = new HashMap();
  private int numBufferedDeleteTerms = 0;

  /**
   * @see IndexWriter#IndexWriter(String, Analyzer, boolean)
   */
  public NewIndexModifier(String path, Analyzer a, boolean create)
      throws IOException {
    super(path, a, create);
  }

  /**
   * @see IndexWriter#IndexWriter(File, Analyzer, boolean)
   */
  public NewIndexModifier(File path, Analyzer a, boolean create)
      throws IOException {
    super(path, a, create);
  }

  /**
   * @see IndexWriter#IndexWriter(Directory, Analyzer, boolean)
   */
  public NewIndexModifier(Directory d, Analyzer a, boolean create)
      throws IOException {
    super(d, a, create);
  }

  /**
   * @see IndexWriter#IndexWriter(String, Analyzer)
   */
  public NewIndexModifier(String path, Analyzer a) throws IOException {
    super(path, a);
  }

  /**
   * @see IndexWriter#IndexWriter(File, Analyzer)
   */
  public NewIndexModifier(File path, Analyzer a) throws IOException {
    super(path, a);
  }

  /**
   * @see IndexWriter#IndexWriter(Directory, Analyzer)
   */
  public NewIndexModifier(Directory d, Analyzer a) throws IOException {
    super(d, a);
  }

  /**
   * Determines the minimal number of delete terms required before the buffered
   * in-memory delete terms are applied and flushed. If there are documents
   * buffered in memory at the time, they are merged and a new Segment is
   * created. The delete terms are applied appropriately.
   * <p>
   * The default value is 10.
   * @throws IllegalArgumentException if maxBufferedDeleteTerms is smaller than
   *         1
   */
  public void setMaxBufferedDeleteTerms(int maxBufferedDeleteTerms) {
    if (maxBufferedDeleteTerms < 1)
      throw new IllegalArgumentException("maxBufferedDeleteTerms must at least be 1");
    this.maxBufferedDeleteTerms = maxBufferedDeleteTerms;
  }

  /**
   * @see #setMaxBufferedDeleteTerms
   */
  public int getMaxBufferedDeleteTerms() {
    return maxBufferedDeleteTerms;
  }

  // for test purpose
  final synchronized int getBufferedDeleteTermsSize() {
    return bufferedDeleteTerms.size();
  }

  // for test purpose
  final synchronized int getNumBufferedDeleteTerms() {
    return numBufferedDeleteTerms;
  }

  /**
   * Updates a document by first deleting all documents containing
   * <code>term</code> and then adding the new document.
   */
  public void updateDocument(Term term, Document doc) throws IOException {
    updateDocument(term, doc, getAnalyzer());
  }

  /**
   * Updates a document by first deleting all documents containing
   * <code>term</code> and then adding the new document.
   */
  public void updateDocument(Term term, Document doc, Analyzer analyzer)
      throws IOException {
    SegmentInfo newSegmentInfo = buildSingleDocSegment(doc, analyzer);
    synchronized (this) {
      bufferDeleteTerm(term);
      ramSegmentInfos.addElement(newSegmentInfo);
      maybeFlushRamSegments();
    }
  }

  /**
   * Deletes all documents containing <code>term</code>.
   */
  public synchronized void deleteDocuments(Term term) throws IOException {
    bufferDeleteTerm(term);
    maybeFlushRamSegments();
  }

  /**
   * Deletes all documents containing any of the terms. All deletes are flushed
   * at the same time.
   */
  public synchronized void deleteDocuments(Term[] terms) throws IOException {
    for (int i = 0; i < terms.length; i++) {
      bufferDeleteTerm(terms[i]);
    }
    maybeFlushRamSegments();
  }

  // buffer a term in bufferedDeleteTerms. bufferedDeleteTerms also records
  // the current number of documents buffered in ram so that the delete term
  // will be applied to those ram segments as well as the disk segments
  private void bufferDeleteTerm(Term term) {
    Num num = (Num)bufferedDeleteTerms.get(term);
    if (num == null) {
      bufferedDeleteTerms.put(term, new Num(getRAMSegmentCount()));
    } else {
      num.setNum(getRAMSegmentCount());
    }
    numBufferedDeleteTerms++;
  }

  // a flush is triggered if enough new documents are buffered or
  // if enough delete terms are buffered
  protected boolean timeToFlushRam() {
    return super.timeToFlushRam()
        || numBufferedDeleteTerms >= maxBufferedDeleteTerms;
  }

  protected boolean anythingToFlushRam() {
    return super.anythingToFlushRam() || bufferedDeleteTerms.size() > 0;
  }

  protected boolean onlyRamDocsToFlush() {
    return super.onlyRamDocsToFlush() && bufferedDeleteTerms.size() == 0;
  }

  protected void doAfterFlushRamSegments(boolean flushedRamSegments)
      throws IOException {
    if (bufferedDeleteTerms.size() > 0) {
      if (getInfoStream() != null)
        getInfoStream().println(
            "flush " + numBufferedDeleteTerms + " buffered terms on "
                + segmentInfos.size() + " segments.");

      if (flushedRamSegments) {
        IndexReader reader = null;
        try {
          reader = SegmentReader.get(segmentInfos.info(segmentInfos.size() - 1));
          reader.setDeleter(getDeleter());

          // apply delete terms to the segment just flushed from ram
          // apply appropriately so that a delete term is only applied to
          // the documents buffered before it, not those buffered after it
          applyDeletesSelectively(bufferedDeleteTerms, reader);
        } finally {
          if (reader != null)
            reader.close();
        }
      }

      int infosEnd = segmentInfos.size();
      if (flushedRamSegments) {
        infosEnd--;
      }

      for (int i = 0; i < infosEnd; i++) {
        IndexReader reader = null;
        try {
          reader = SegmentReader.get(segmentInfos.info(i));
          reader.setDeleter(getDeleter());

          // apply delete terms to disk segments
          // except the one just flushed from ram
          applyDeletes(bufferedDeleteTerms, reader);
        } finally {
          if (reader != null)
            reader.close();
        }
      }

      // clean up bufferedDeleteTerms
      bufferedDeleteTerms.clear();
      numBufferedDeleteTerms = 0;
    }
  }

  // apply buffered delete terms to the segment just flushed from ram
  // apply appropriately so that a delete term is only applied to
  // the documents buffered before it, not those buffered after it
  private final void applyDeletesSelectively(HashMap deleteTerms,
      IndexReader reader) throws IOException {
    Iterator iter = deleteTerms.entrySet().iterator();
    while (iter.hasNext()) {
      Entry entry = (Entry)iter.next();
      Term term = (Term)entry.getKey();

      TermDocs docs = reader.termDocs(term);
      if (docs != null) {
        int num = ((Num)entry.getValue()).getNum();
        try {
          while (docs.next()) {
            int doc = docs.doc();
            if (doc >= num) {
              break;
            }
            reader.deleteDocument(doc);
          }
        } finally {
          docs.close();
        }
      }
    }
  }

  // apply buffered delete terms to disk segments
  // except the one just flushed from ram
  private final void applyDeletes(HashMap deleteTerms, IndexReader reader)
      throws IOException {
    Iterator iter = deleteTerms.entrySet().iterator();
    while (iter.hasNext()) {
      Entry entry = (Entry)iter.next();
      Term term = (Term)entry.getKey();
      reader.deleteDocuments(term);
    }
  }
}
