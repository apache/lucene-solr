package org.apache.lucene.index;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001, 2002, 2003 The Apache Software Foundation.
 * All rights reserved.
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
import java.io.File;
import java.util.Collection;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;          // for javadoc
import org.apache.lucene.search.Similarity;

/** IndexReader is an abstract class, providing an interface for accessing an
 index.  Search of an index is done entirely through this abstract interface,
 so that any subclass which implements it is searchable.

 <p> Concrete subclasses of IndexReader are usually constructed with a call to
 the static method {@link #open}.

 <p> For efficiency, in this API documents are often referred to via
 <i>document numbers</i>, non-negative integers which each name a unique
 document in the index.  These document numbers are ephemeral--they may change
 as documents are added to and deleted from an index.  Clients should thus not
 rely on a given document having the same number between sessions.

 @author Doug Cutting
 @version $Id$
*/
public abstract class IndexReader {
  protected IndexReader(Directory directory) {
    this.directory = directory;
    stale = false;
    segmentInfos = null;
  }

  private Directory directory;
  private Lock writeLock;
  SegmentInfos segmentInfos = null;
  private boolean stale = false;

  /** Returns an IndexReader reading the index in an FSDirectory in the named
   path. */
  public static IndexReader open(String path) throws IOException {
    return open(FSDirectory.getDirectory(path, false));
  }

  /** Returns an IndexReader reading the index in an FSDirectory in the named
   path. */
  public static IndexReader open(File path) throws IOException {
    return open(FSDirectory.getDirectory(path, false));
  }

  /** Returns an IndexReader reading the index in the given Directory. */
  public static IndexReader open(final Directory directory) throws IOException {
    synchronized (directory) {			  // in- & inter-process sync
      return (IndexReader)new Lock.With(
          directory.makeLock(IndexWriter.COMMIT_LOCK_NAME),
          IndexWriter.COMMIT_LOCK_TIMEOUT) {
          public Object doBody() throws IOException {
            SegmentInfos infos = new SegmentInfos();
            infos.read(directory);
            if (infos.size() == 1) {		  // index is optimized
              return new SegmentReader(infos, infos.info(0), true);
            } else {
              IndexReader[] readers = new IndexReader[infos.size()];
              for (int i = 0; i < infos.size(); i++)
                readers[i] = new SegmentReader(infos, infos.info(i), i==infos.size()-1);
              return new MultiReader(directory, readers);
            }
          }
        }.run();
    }
  }

  /** Returns the directory this index resides in. */
  public Directory directory() { return directory; }

  /** 
   * Returns the time the index in the named directory was last modified. 
   * 
   * <p>Synchronization of IndexReader and IndexWriter instances is 
   * no longer done via time stamps of the segments file since the time resolution 
   * depends on the hardware platform. Instead, a version number is maintained
   * within the segments file, which is incremented everytime when the index is
   * changed.</p>
   * 
   * @deprecated  Replaced by {@link #getCurrentVersion(String)}
   * */
  public static long lastModified(String directory) throws IOException {
    return lastModified(new File(directory));
  }

  /** 
   * Returns the time the index in the named directory was last modified. 
   * 
   * <p>Synchronization of IndexReader and IndexWriter instances is 
   * no longer done via time stamps of the segments file since the time resolution 
   * depends on the hardware platform. Instead, a version number is maintained
   * within the segments file, which is incremented everytime when the index is
   * changed.</p>
   * 
   * @deprecated  Replaced by {@link #getCurrentVersion(File)}
   * */
  public static long lastModified(File directory) throws IOException {
    return FSDirectory.fileModified(directory, "segments");
  }

  /** 
   * Returns the time the index in the named directory was last modified. 
   * 
   * <p>Synchronization of IndexReader and IndexWriter instances is 
   * no longer done via time stamps of the segments file since the time resolution 
   * depends on the hardware platform. Instead, a version number is maintained
   * within the segments file, which is incremented everytime when the index is
   * changed.</p>
   * 
   * @deprecated  Replaced by {@link #getCurrentVersion(Directory)}
   * */
  public static long lastModified(Directory directory) throws IOException {
    return directory.fileModified("segments");
  }

  /**
   * Reads version number from segments files. The version number counts the
   * number of changes of the index.
   * 
   * @param directory where the index resides.
   * @return version number.
   * @throws IOException if segments file cannot be read
   */
  public static long getCurrentVersion(String directory) throws IOException {
    return getCurrentVersion(new File(directory));
  }

  /**
   * Reads version number from segments files. The version number counts the
   * number of changes of the index.
   * 
   * @param directory where the index resides.
   * @return version number.
   * @throws IOException if segments file cannot be read
   */
  public static long getCurrentVersion(File directory) throws IOException {
    Directory dir = FSDirectory.getDirectory(directory, false);
    long version = getCurrentVersion(dir);
    dir.close();
    return version;
  }

  /**
   * Reads version number from segments files. The version number counts the
   * number of changes of the index.
   * 
   * @param directory where the index resides.
   * @return version number.
   * @throws IOException if segments file cannot be read.
   */
  public static long getCurrentVersion(Directory directory) throws IOException {
    return SegmentInfos.readCurrentVersion(directory);
  }

  /** Return an array of term frequency vectors for the specified document.
   *  The array contains a vector for each vectorized field in the document.
   *  Each vector vector contains term numbers and frequencies for all terms
   *  in a given vectorized field.
   *  If no such fields existed, the method returns null.
   *
   * @see Field#isTermVectorStored()
   */
  abstract public TermFreqVector[] getTermFreqVectors(int docNumber)
          throws IOException;

  /** Return a term frequency vector for the specified document and field. The
   *  vector returned contains term numbers and frequencies for all terms in
   *  the specified field of this document, if the field had storeTermVector
   *  flag set.  If the flag was not set, the method returns null.
   *
   * @see Field#isTermVectorStored()
   */
  abstract public TermFreqVector getTermFreqVector(int docNumber, String field)
          throws IOException;
 
  /**
   * Returns <code>true</code> if an index exists at the specified directory.
   * If the directory does not exist or if there is no index in it.
   * <code>false</code> is returned.
   * @param  directory the directory to check for an index
   * @return <code>true</code> if an index exists; <code>false</code> otherwise
   */
  public static boolean indexExists(String directory) {
    return (new File(directory, "segments")).exists();
  }

  /**
   * Returns <code>true</code> if an index exists at the specified directory.
   * If the directory does not exist or if there is no index in it.
   * @param  directory the directory to check for an index
   * @return <code>true</code> if an index exists; <code>false</code> otherwise
   */
  public static boolean indexExists(File directory) {
    return (new File(directory, "segments")).exists();
  }

  /**
   * Returns <code>true</code> if an index exists at the specified directory.
   * If the directory does not exist or if there is no index in it.
   * @param  directory the directory to check for an index
   * @return <code>true</code> if an index exists; <code>false</code> otherwise
   * @throws IOException if there is a problem with accessing the index
   */
  public static boolean indexExists(Directory directory) throws IOException {
    return directory.fileExists("segments");
  }

  /** Returns the number of documents in this index. */
  public abstract int numDocs();

  /** Returns one greater than the largest possible document number.
   This may be used to, e.g., determine how big to allocate an array which
   will have an element for every document number in an index.
   */
  public abstract int maxDoc();

  /** Returns the stored fields of the <code>n</code><sup>th</sup>
   <code>Document</code> in this index. */
  public abstract Document document(int n) throws IOException;

  /** Returns true if document <i>n</i> has been deleted */
  public abstract boolean isDeleted(int n);

  /** Returns true if any documents have been deleted */
  public abstract boolean hasDeletions();
  
  /** Returns the byte-encoded normalization factor for the named field of
   * every document.  This is used by the search code to score documents.
   *
   * @see Field#setBoost(float)
   */
  public abstract byte[] norms(String field) throws IOException;

  /** Reads the byte-encoded normalization factor for the named field of every
   *  document.  This is used by the search code to score documents.
   *
   * @see Field#setBoost(float)
   */
  public abstract void norms(String field, byte[] bytes, int offset)
    throws IOException;

  /** Expert: Resets the normalization factor for the named field of the named
   * document.  The norm represents the product of the field's {@link
   * Field#setBoost(float) boost} and its {@link Similarity#lengthNorm(String,
          * int) length normalization}.  Thus, to preserve the length normalization
   * values when resetting this, one should base the new value upon the old.
   *
   * @see #norms(String)
   * @see Similarity#decodeNorm(byte)
   */
  public abstract void setNorm(int doc, String field, byte value)
          throws IOException;

  /** Expert: Resets the normalization factor for the named field of the named
   * document.
   *
   * @see #norms(String)
   * @see Similarity#decodeNorm(byte)
   */
  public void setNorm(int doc, String field, float value)
          throws IOException {
    setNorm(doc, field, Similarity.encodeNorm(value));
  }


  /** Returns an enumeration of all the terms in the index.
   The enumeration is ordered by Term.compareTo().  Each term
   is greater than all that precede it in the enumeration.
   */
  public abstract TermEnum terms() throws IOException;

  /** Returns an enumeration of all terms after a given term.
   The enumeration is ordered by Term.compareTo().  Each term
   is greater than all that precede it in the enumeration.
   */
  public abstract TermEnum terms(Term t) throws IOException;

  /** Returns the number of documents containing the term <code>t</code>. */
  public abstract int docFreq(Term t) throws IOException;

  /** Returns an enumeration of all the documents which contain
   <code>term</code>. For each document, the document number, the frequency of
   the term in that document is also provided, for use in search scoring.
   Thus, this method implements the mapping:
   <p><ul>
   Term &nbsp;&nbsp; =&gt; &nbsp;&nbsp; &lt;docNum, freq&gt;<sup>*</sup>
   </ul>
   <p>The enumeration is ordered by document number.  Each document number
   is greater than all that precede it in the enumeration.
   */
  public TermDocs termDocs(Term term) throws IOException {
    TermDocs termDocs = termDocs();
    termDocs.seek(term);
    return termDocs;
  }

  /** Returns an unpositioned {@link TermDocs} enumerator. */
  public abstract TermDocs termDocs() throws IOException;

  /** Returns an enumeration of all the documents which contain
   <code>term</code>.  For each document, in addition to the document number
   and frequency of the term in that document, a list of all of the ordinal
   positions of the term in the document is available.  Thus, this method
   implements the mapping:

   <p><ul>
   Term &nbsp;&nbsp; =&gt; &nbsp;&nbsp; &lt;docNum, freq,
   &lt;pos<sub>1</sub>, pos<sub>2</sub>, ...
   pos<sub>freq-1</sub>&gt;
   &gt;<sup>*</sup>
   </ul>
   <p> This positional information faciliates phrase and proximity searching.
   <p>The enumeration is ordered by document number.  Each document number is
   greater than all that precede it in the enumeration.
   */
  public TermPositions termPositions(Term term) throws IOException {
    TermPositions termPositions = termPositions();
    termPositions.seek(term);
    return termPositions;
  }

  /** Returns an unpositioned {@link TermPositions} enumerator. */
  public abstract TermPositions termPositions() throws IOException;

  /** Deletes the document numbered <code>docNum</code>.  Once a document is
   deleted it will not appear in TermDocs or TermPostitions enumerations.
   Attempts to read its field with the {@link #document}
   method will result in an error.  The presence of this document may still be
   reflected in the {@link #docFreq} statistic, though
   this will be corrected eventually as the index is further modified.
   */
  public final synchronized void delete(int docNum) throws IOException {
    if (stale)
      throw new IOException("IndexReader out of date and no longer valid for deletion");

    if (writeLock == null) {
      Lock writeLock = directory.makeLock(IndexWriter.WRITE_LOCK_NAME);
      if (!writeLock.obtain(IndexWriter.WRITE_LOCK_TIMEOUT)) // obtain write lock
        throw new IOException("Index locked for write: " + writeLock);
      this.writeLock = writeLock;

      // we have to check whether index has changed since this reader was opened.
      // if so, this reader is no longer valid for deletion
      if (segmentInfos != null && SegmentInfos.readCurrentVersion(directory) > segmentInfos.getVersion()) {
        stale = true;
        this.writeLock.release();
        this.writeLock = null;
        throw new IOException("IndexReader out of date and no longer valid for deletion");
      }
    }
    doDelete(docNum);
  }

  /** Implements deletion of the document numbered <code>docNum</code>.
   * Applications should call {@link #delete(int)} or {@link #delete(Term)}.
   */
  protected abstract void doDelete(int docNum) throws IOException;

  /** Deletes all documents containing <code>term</code>.
   This is useful if one uses a document field to hold a unique ID string for
   the document.  Then to delete such a document, one merely constructs a
   term with the appropriate field and the unique ID string as its text and
   passes it to this method.  Returns the number of documents deleted.
   */
  public final int delete(Term term) throws IOException {
    TermDocs docs = termDocs(term);
    if (docs == null) return 0;
    int n = 0;
    try {
      while (docs.next()) {
        delete(docs.doc());
        n++;
      }
    } finally {
      docs.close();
    }
    return n;
  }

  /** Undeletes all documents currently marked as deleted in this index.*/
  public abstract void undeleteAll() throws IOException;

  /**
   * Closes files associated with this index.
   * Also saves any new deletions to disk.
   * No other methods should be called after this has been called.
   */
  public final synchronized void close() throws IOException {
    doClose();
    if (writeLock != null) {
      writeLock.release();  // release write lock
      writeLock = null;
    }
  }

  /** Implements close. */
  protected abstract void doClose() throws IOException;

  /** Release the write lock, if needed. */
  protected final void finalize() throws IOException {
    if (writeLock != null) {
      writeLock.release();                        // release write lock
      writeLock = null;
    }
  }
  
  /**
   * Returns a list of all unique field names that exist in the index pointed
   * to by this IndexReader.
   * @return Collection of Strings indicating the names of the fields
   * @throws IOException if there is a problem with accessing the index
   */
  public abstract Collection getFieldNames() throws IOException;

  /**
   * Returns a list of all unique field names that exist in the index pointed
   * to by this IndexReader.  The boolean argument specifies whether the fields
   * returned are indexed or not.
   * @param indexed <code>true</code> if only indexed fields should be returned;
   *                <code>false</code> if only unindexed fields should be returned.
   * @return Collection of Strings indicating the names of the fields
   * @throws IOException if there is a problem with accessing the index
   */
  public abstract Collection getFieldNames(boolean indexed) throws IOException;

  /**
   * 
   * @param storedTermVector if true, returns only Indexed fields that have term vector info, 
   *                        else only indexed fields without term vector info 
   * @return Collection of Strings indicating the names of the fields
   */ 
  public abstract Collection getIndexedFieldNames(boolean storedTermVector);

  /**
   * Returns <code>true</code> iff the index in the named directory is
   * currently locked.
   * @param directory the directory to check for a lock
   * @throws IOException if there is a problem with accessing the index
   */
  public static boolean isLocked(Directory directory) throws IOException {
    return
            directory.makeLock(IndexWriter.WRITE_LOCK_NAME).isLocked() ||
            directory.makeLock(IndexWriter.COMMIT_LOCK_NAME).isLocked();

  }

  /**
   * Returns <code>true</code> iff the index in the named directory is
   * currently locked.
   * @param directory the directory to check for a lock
   * @throws IOException if there is a problem with accessing the index
   */
  public static boolean isLocked(String directory) throws IOException {
    return isLocked(FSDirectory.getDirectory(directory, false));
  }

  /**
   * Forcibly unlocks the index in the named directory.
   * <P>
   * Caution: this should only be used by failure recovery code,
   * when it is known that no other process nor thread is in fact
   * currently accessing this index.
   */
  public static void unlock(Directory directory) throws IOException {
    directory.makeLock(IndexWriter.WRITE_LOCK_NAME).release();
    directory.makeLock(IndexWriter.COMMIT_LOCK_NAME).release();
  }
}
