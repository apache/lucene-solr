package org.apache.lucene.index;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.Lock;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/** IndexReader is an abstract class, providing an interface for accessing an
 index.  Search of an index is done entirely through this abstract interface,
 so that any subclass which implements it is searchable.

 <p> Concrete subclasses of IndexReader are usually constructed with a call to
 one of the static <code>open()</code> methods, e.g. {@link #open(String)}.

 <p> For efficiency, in this API documents are often referred to via
 <i>document numbers</i>, non-negative integers which each name a unique
 document in the index.  These document numbers are ephemeral--they may change
 as documents are added to and deleted from an index.  Clients should thus not
 rely on a given document having the same number between sessions.

 <p> An IndexReader can be opened on a directory for which an IndexWriter is
 opened already, but it cannot be used to delete documents from the index then.

 @author Doug Cutting
 @version $Id$
*/
public abstract class IndexReader {

  public static final class FieldOption {
    private String option;
    private FieldOption() { }
    private FieldOption(String option) {
      this.option = option;
    }
    public String toString() {
      return this.option;
    }
    // all fields
    public static final FieldOption ALL = new FieldOption ("ALL");
    // all indexed fields
    public static final FieldOption INDEXED = new FieldOption ("INDEXED");
    // all fields which are not indexed
    public static final FieldOption UNINDEXED = new FieldOption ("UNINDEXED");
    // all fields which are indexed with termvectors enables
    public static final FieldOption INDEXED_WITH_TERMVECTOR = new FieldOption ("INDEXED_WITH_TERMVECTOR");
    // all fields which are indexed but don't have termvectors enabled
    public static final FieldOption INDEXED_NO_TERMVECTOR = new FieldOption ("INDEXED_NO_TERMVECTOR");
    // all fields where termvectors are enabled. Please note that only standard termvector fields are returned
    public static final FieldOption TERMVECTOR = new FieldOption ("TERMVECTOR");
    // all field with termvectors wiht positions enabled
    public static final FieldOption TERMVECTOR_WITH_POSITION = new FieldOption ("TERMVECTOR_WITH_POSITION");
    // all fields where termvectors with offset position are set
    public static final FieldOption TERMVECTOR_WITH_OFFSET = new FieldOption ("TERMVECTOR_WITH_OFFSET");
    // all fields where termvectors with offset and position values set
    public static final FieldOption TERMVECTOR_WITH_POSITION_OFFSET = new FieldOption ("TERMVECTOR_WITH_POSITION_OFFSET");
  }

  /**
   * Constructor used if IndexReader is not owner of its directory. 
   * This is used for IndexReaders that are used within other IndexReaders that take care or locking directories.
   * 
   * @param directory Directory where IndexReader files reside.
   */
  protected IndexReader(Directory directory) {
    this.directory = directory;
  }

  /**
   * Constructor used if IndexReader is owner of its directory.
   * If IndexReader is owner of its directory, it locks its directory in case of write operations.
   * 
   * @param directory Directory where IndexReader files reside.
   * @param segmentInfos Used for write-l
   * @param closeDirectory
   */
  IndexReader(Directory directory, SegmentInfos segmentInfos, boolean closeDirectory) {
    init(directory, segmentInfos, closeDirectory, true);
  }

  void init(Directory directory, SegmentInfos segmentInfos, boolean closeDirectory, boolean directoryOwner) {
    this.directory = directory;
    this.segmentInfos = segmentInfos;
    this.directoryOwner = directoryOwner;
    this.closeDirectory = closeDirectory;
  }

  private Directory directory;
  private boolean directoryOwner;
  private boolean closeDirectory;

  private SegmentInfos segmentInfos;
  private Lock writeLock;
  private boolean stale;
  private boolean hasChanges;


  /** Returns an IndexReader reading the index in an FSDirectory in the named
   path. */
  public static IndexReader open(String path) throws IOException {
    return open(FSDirectory.getDirectory(path, false), true);
  }

  /** Returns an IndexReader reading the index in an FSDirectory in the named
   path. */
  public static IndexReader open(File path) throws IOException {
    return open(FSDirectory.getDirectory(path, false), true);
  }

  /** Returns an IndexReader reading the index in the given Directory. */
  public static IndexReader open(final Directory directory) throws IOException {
    return open(directory, false);
  }

  private static IndexReader open(final Directory directory, final boolean closeDirectory) throws IOException {
    synchronized (directory) {			  // in- & inter-process sync
      return (IndexReader)new Lock.With(
          directory.makeLock(IndexWriter.COMMIT_LOCK_NAME),
          IndexWriter.COMMIT_LOCK_TIMEOUT) {
          public Object doBody() throws IOException {
            SegmentInfos infos = new SegmentInfos();
            infos.read(directory);
            if (infos.size() == 1) {		  // index is optimized
              return SegmentReader.get(infos, infos.info(0), closeDirectory);
            }
            IndexReader[] readers = new IndexReader[infos.size()];
            for (int i = 0; i < infos.size(); i++)
              readers[i] = SegmentReader.get(infos.info(i));
            return new MultiReader(directory, infos, closeDirectory, readers);

          }
        }.run();
    }
  }

  /** Returns the directory this index resides in. */
  public Directory directory() { return directory; }

  /**
   * Returns the time the index in the named directory was last modified.
   * Do not use this to check whether the reader is still up-to-date, use
   * {@link #isCurrent()} instead. 
   */
  public static long lastModified(String directory) throws IOException {
    return lastModified(new File(directory));
  }

  /**
   * Returns the time the index in the named directory was last modified. 
   * Do not use this to check whether the reader is still up-to-date, use
   * {@link #isCurrent()} instead. 
   */
  public static long lastModified(File directory) throws IOException {
    return FSDirectory.fileModified(directory, IndexFileNames.SEGMENTS);
  }

  /**
   * Returns the time the index in the named directory was last modified. 
   * Do not use this to check whether the reader is still up-to-date, use
   * {@link #isCurrent()} instead. 
   */
  public static long lastModified(Directory directory) throws IOException {
    return directory.fileModified(IndexFileNames.SEGMENTS);
  }

  /**
   * Reads version number from segments files. The version number is
   * initialized with a timestamp and then increased by one for each change of
   * the index.
   * 
   * @param directory where the index resides.
   * @return version number.
   * @throws IOException if segments file cannot be read
   */
  public static long getCurrentVersion(String directory) throws IOException {
    return getCurrentVersion(new File(directory));
  }

  /**
   * Reads version number from segments files. The version number is
   * initialized with a timestamp and then increased by one for each change of
   * the index.
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
   * Reads version number from segments files. The version number is
   * initialized with a timestamp and then increased by one for each change of
   * the index.
   * 
   * @param directory where the index resides.
   * @return version number.
   * @throws IOException if segments file cannot be read.
   */
  public static long getCurrentVersion(Directory directory) throws IOException {
    synchronized (directory) {                 // in- & inter-process sync
      Lock commitLock=directory.makeLock(IndexWriter.COMMIT_LOCK_NAME);

      boolean locked=false;

      try {
         locked=commitLock.obtain(IndexWriter.COMMIT_LOCK_TIMEOUT);

         return SegmentInfos.readCurrentVersion(directory);
      } finally {
        if (locked) {
          commitLock.release();
        }
      }
    }
  }

  /**
   * Version number when this IndexReader was opened.
   */
  public long getVersion() {
    return segmentInfos.getVersion();
  }

  /**
   * Check whether this IndexReader still works on a current version of the index.
   * If this is not the case you will need to re-open the IndexReader to
   * make sure you see the latest changes made to the index.
   * 
   * @throws IOException
   */
  public boolean isCurrent() throws IOException {
    synchronized (directory) {                 // in- & inter-process sync
      Lock commitLock=directory.makeLock(IndexWriter.COMMIT_LOCK_NAME);

      boolean locked=false;

      try {
         locked=commitLock.obtain(IndexWriter.COMMIT_LOCK_TIMEOUT);

         return SegmentInfos.readCurrentVersion(directory) == segmentInfos.getVersion();
      } finally {
        if (locked) {
          commitLock.release();
        }
      }
    }
  }

  /**
   *  Return an array of term frequency vectors for the specified document.
   *  The array contains a vector for each vectorized field in the document.
   *  Each vector contains terms and frequencies for all terms in a given vectorized field.
   *  If no such fields existed, the method returns null. The term vectors that are
   * returned my either be of type TermFreqVector or of type TermPositionsVector if
   * positions or offsets have been stored.
   * 
   * @param docNumber document for which term frequency vectors are returned
   * @return array of term frequency vectors. May be null if no term vectors have been
   *  stored for the specified document.
   * @throws IOException if index cannot be accessed
   * @see org.apache.lucene.document.Field.TermVector
   */
  abstract public TermFreqVector[] getTermFreqVectors(int docNumber)
          throws IOException;


  /**
   *  Return a term frequency vector for the specified document and field. The
   *  returned vector contains terms and frequencies for the terms in
   *  the specified field of this document, if the field had the storeTermVector
   *  flag set. If termvectors had been stored with positions or offsets, a 
   *  TermPositionsVector is returned.
   * 
   * @param docNumber document for which the term frequency vector is returned
   * @param field field for which the term frequency vector is returned.
   * @return term frequency vector May be null if field does not exist in the specified
   * document or term vector was not stored.
   * @throws IOException if index cannot be accessed
   * @see org.apache.lucene.document.Field.TermVector
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
    return (new File(directory, IndexFileNames.SEGMENTS)).exists();
  }

  /**
   * Returns <code>true</code> if an index exists at the specified directory.
   * If the directory does not exist or if there is no index in it.
   * @param  directory the directory to check for an index
   * @return <code>true</code> if an index exists; <code>false</code> otherwise
   */
  public static boolean indexExists(File directory) {
    return (new File(directory, IndexFileNames.SEGMENTS)).exists();
  }

  /**
   * Returns <code>true</code> if an index exists at the specified directory.
   * If the directory does not exist or if there is no index in it.
   * @param  directory the directory to check for an index
   * @return <code>true</code> if an index exists; <code>false</code> otherwise
   * @throws IOException if there is a problem with accessing the index
   */
  public static boolean indexExists(Directory directory) throws IOException {
    return directory.fileExists(IndexFileNames.SEGMENTS);
  }

  /** Returns the number of documents in this index. */
  public abstract int numDocs();

  /** Returns one greater than the largest possible document number.
   * This may be used to, e.g., determine how big to allocate an array which
   * will have an element for every document number in an index.
   */
  public abstract int maxDoc();

  /** Returns the stored fields of the <code>n</code><sup>th</sup>
   <code>Document</code> in this index. */
  public Document document(int n) throws IOException{
    return document(n, null);
  }

  /**
   * Get the {@link org.apache.lucene.document.Document} at the <code>n</code><sup>th</sup> position. The {@link org.apache.lucene.document.FieldSelector}
   * may be used to determine what {@link org.apache.lucene.document.Field}s to load and how they should be loaded.
   * 
   * <b>NOTE:</b> If this Reader (more specifically, the underlying {@link FieldsReader} is closed before the lazy {@link org.apache.lucene.document.Field} is
   * loaded an exception may be thrown.  If you want the value of a lazy {@link org.apache.lucene.document.Field} to be available after closing you must
   * explicitly load it or fetch the Document again with a new loader.
   * 
   *  
   * @param n Get the document at the <code>n</code><sup>th</sup> position
   * @param fieldSelector The {@link org.apache.lucene.document.FieldSelector} to use to determine what Fields should be loaded on the Document.  May be null, in which case all Fields will be loaded.
   * @return The stored fields of the {@link org.apache.lucene.document.Document} at the nth position
   * @throws IOException If there is a problem reading this document
   * 
   * @see org.apache.lucene.document.Fieldable
   * @see org.apache.lucene.document.FieldSelector
   * @see org.apache.lucene.document.SetBasedFieldSelector
   * @see org.apache.lucene.document.LoadFirstFieldSelector
   */
  //When we convert to JDK 1.5 make this Set<String>
  public abstract Document document(int n, FieldSelector fieldSelector) throws IOException;
  
  

  /** Returns true if document <i>n</i> has been deleted */
  public abstract boolean isDeleted(int n);

  /** Returns true if any documents have been deleted */
  public abstract boolean hasDeletions();

  /** Returns true if there are norms stored for this field. */
  public boolean hasNorms(String field) throws IOException {
    // backward compatible implementation.
    // SegmentReader has an efficient implementation.
    return norms(field) != null;
  }

  /** Returns the byte-encoded normalization factor for the named field of
   * every document.  This is used by the search code to score documents.
   *
   * @see org.apache.lucene.document.Field#setBoost(float)
   */
  public abstract byte[] norms(String field) throws IOException;

  /** Reads the byte-encoded normalization factor for the named field of every
   *  document.  This is used by the search code to score documents.
   *
   * @see org.apache.lucene.document.Field#setBoost(float)
   */
  public abstract void norms(String field, byte[] bytes, int offset)
    throws IOException;

  /** Expert: Resets the normalization factor for the named field of the named
   * document.  The norm represents the product of the field's {@link
   * Fieldable#setBoost(float) boost} and its {@link Similarity#lengthNorm(String,
   * int) length normalization}.  Thus, to preserve the length normalization
   * values when resetting this, one should base the new value upon the old.
   *
   * @see #norms(String)
   * @see Similarity#decodeNorm(byte)
   */
  public final synchronized  void setNorm(int doc, String field, byte value)
          throws IOException{
    if(directoryOwner)
      aquireWriteLock();
    doSetNorm(doc, field, value);
    hasChanges = true;
  }

  /** Implements setNorm in subclass.*/
  protected abstract void doSetNorm(int doc, String field, byte value)
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
   * The enumeration is ordered by Term.compareTo().  Each term
   * is greater than all that precede it in the enumeration.
   */
  public abstract TermEnum terms() throws IOException;

  /** Returns an enumeration of all terms after a given term.
   * The enumeration is ordered by Term.compareTo().  Each term
   * is greater than all that precede it in the enumeration.
   */
  public abstract TermEnum terms(Term t) throws IOException;

  /** Returns the number of documents containing the term <code>t</code>. */
  public abstract int docFreq(Term t) throws IOException;

  /** Returns an enumeration of all the documents which contain
   * <code>term</code>. For each document, the document number, the frequency of
   * the term in that document is also provided, for use in search scoring.
   * Thus, this method implements the mapping:
   * <p><ul>
   * Term &nbsp;&nbsp; =&gt; &nbsp;&nbsp; &lt;docNum, freq&gt;<sup>*</sup>
   * </ul>
   * <p>The enumeration is ordered by document number.  Each document number
   * is greater than all that precede it in the enumeration.
   */
  public TermDocs termDocs(Term term) throws IOException {
    TermDocs termDocs = termDocs();
    termDocs.seek(term);
    return termDocs;
  }

  /** Returns an unpositioned {@link TermDocs} enumerator. */
  public abstract TermDocs termDocs() throws IOException;

  /** Returns an enumeration of all the documents which contain
   * <code>term</code>.  For each document, in addition to the document number
   * and frequency of the term in that document, a list of all of the ordinal
   * positions of the term in the document is available.  Thus, this method
   * implements the mapping:
   *
   * <p><ul>
   * Term &nbsp;&nbsp; =&gt; &nbsp;&nbsp; &lt;docNum, freq,
   * &lt;pos<sub>1</sub>, pos<sub>2</sub>, ...
   * pos<sub>freq-1</sub>&gt;
   * &gt;<sup>*</sup>
   * </ul>
   * <p> This positional information faciliates phrase and proximity searching.
   * <p>The enumeration is ordered by document number.  Each document number is
   * greater than all that precede it in the enumeration.
   */
  public TermPositions termPositions(Term term) throws IOException {
    TermPositions termPositions = termPositions();
    termPositions.seek(term);
    return termPositions;
  }

  /** Returns an unpositioned {@link TermPositions} enumerator. */
  public abstract TermPositions termPositions() throws IOException;

  /**
   * Tries to acquire the WriteLock on this directory.
   * this method is only valid if this IndexReader is directory owner.
   * 
   * @throws IOException If WriteLock cannot be acquired.
   */
  private void aquireWriteLock() throws IOException {
    if (stale)
      throw new IOException("IndexReader out of date and no longer valid for delete, undelete, or setNorm operations");

    if (writeLock == null) {
      Lock writeLock = directory.makeLock(IndexWriter.WRITE_LOCK_NAME);
      if (!writeLock.obtain(IndexWriter.WRITE_LOCK_TIMEOUT)) // obtain write lock
        throw new IOException("Index locked for write: " + writeLock);
      this.writeLock = writeLock;

      // we have to check whether index has changed since this reader was opened.
      // if so, this reader is no longer valid for deletion
      if (SegmentInfos.readCurrentVersion(directory) > segmentInfos.getVersion()) {
        stale = true;
        this.writeLock.release();
        this.writeLock = null;
        throw new IOException("IndexReader out of date and no longer valid for delete, undelete, or setNorm operations");
      }
    }
  }


  /** Deletes the document numbered <code>docNum</code>.  Once a document is
   * deleted it will not appear in TermDocs or TermPostitions enumerations.
   * Attempts to read its field with the {@link #document}
   * method will result in an error.  The presence of this document may still be
   * reflected in the {@link #docFreq} statistic, though
   * this will be corrected eventually as the index is further modified.
   */
  public final synchronized void deleteDocument(int docNum) throws IOException {
    if(directoryOwner)
      aquireWriteLock();
    doDelete(docNum);
    hasChanges = true;
  }


  /** Implements deletion of the document numbered <code>docNum</code>.
   * Applications should call {@link #deleteDocument(int)} or {@link #deleteDocuments(Term)}.
   */
  protected abstract void doDelete(int docNum) throws IOException;


  /** Deletes all documents containing <code>term</code>.
   * This is useful if one uses a document field to hold a unique ID string for
   * the document.  Then to delete such a document, one merely constructs a
   * term with the appropriate field and the unique ID string as its text and
   * passes it to this method.
   * See {@link #deleteDocument(int)} for information about when this deletion will 
   * become effective.
   * @return the number of documents deleted
   */
  public final int deleteDocuments(Term term) throws IOException {
    TermDocs docs = termDocs(term);
    if (docs == null) return 0;
    int n = 0;
    try {
      while (docs.next()) {
        deleteDocument(docs.doc());
        n++;
      }
    } finally {
      docs.close();
    }
    return n;
  }

  /** Undeletes all documents currently marked as deleted in this index.*/
  public final synchronized void undeleteAll() throws IOException{
    if(directoryOwner)
      aquireWriteLock();
    doUndeleteAll();
    hasChanges = true;
  }

  /** Implements actual undeleteAll() in subclass. */
  protected abstract void doUndeleteAll() throws IOException;

  /**
   * Commit changes resulting from delete, undeleteAll, or setNorm operations
   * 
   * @throws IOException
   */
  protected final synchronized void commit() throws IOException{
    if(hasChanges){
      if(directoryOwner){
        synchronized (directory) {      // in- & inter-process sync
           new Lock.With(directory.makeLock(IndexWriter.COMMIT_LOCK_NAME),
                   IndexWriter.COMMIT_LOCK_TIMEOUT) {
             public Object doBody() throws IOException {
               doCommit();
               segmentInfos.write(directory);
               return null;
             }
           }.run();
         }
        if (writeLock != null) {
          writeLock.release();  // release write lock
          writeLock = null;
        }
      }
      else
        doCommit();
    }
    hasChanges = false;
  }

  /** Implements commit. */
  protected abstract void doCommit() throws IOException;

  /**
   * Closes files associated with this index.
   * Also saves any new deletions to disk.
   * No other methods should be called after this has been called.
   */
  public final synchronized void close() throws IOException {
    commit();
    doClose();
    if(closeDirectory)
      directory.close();
  }

  /** Implements close. */
  protected abstract void doClose() throws IOException;

  /** Release the write lock, if needed. */
  protected void finalize() {
    if (writeLock != null) {
      writeLock.release();                        // release write lock
      writeLock = null;
    }
  }


  /**
   * Get a list of unique field names that exist in this index and have the specified
   * field option information.
   * @param fldOption specifies which field option should be available for the returned fields
   * @return Collection of Strings indicating the names of the fields.
   * @see IndexReader.FieldOption
   */
  public abstract Collection getFieldNames(FieldOption fldOption);

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
    Directory dir = FSDirectory.getDirectory(directory, false);
    boolean result = isLocked(dir);
    dir.close();
    return result;
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

  /**
   * Prints the filename and size of each file within a given compound file.
   * Add the -extract flag to extract files to the current working directory.
   * In order to make the extracted version of the index work, you have to copy
   * the segments file from the compound index into the directory where the extracted files are stored.
   * @param args Usage: org.apache.lucene.index.IndexReader [-extract] &lt;cfsfile&gt;
   */
  public static void main(String [] args) {
    String filename = null;
    boolean extract = false;

    for (int i = 0; i < args.length; ++i) {
      if (args[i].equals("-extract")) {
        extract = true;
      } else if (filename == null) {
        filename = args[i];
      }
    }

    if (filename == null) {
      System.out.println("Usage: org.apache.lucene.index.IndexReader [-extract] <cfsfile>");
      return;
    }

    Directory dir = null;
    CompoundFileReader cfr = null;

    try {
      File file = new File(filename);
      String dirname = file.getAbsoluteFile().getParent();
      filename = file.getName();
      dir = FSDirectory.getDirectory(dirname, false);
      cfr = new CompoundFileReader(dir, filename);

      String [] files = cfr.list();
      Arrays.sort(files);   // sort the array of filename so that the output is more readable

      for (int i = 0; i < files.length; ++i) {
        long len = cfr.fileLength(files[i]);

        if (extract) {
          System.out.println("extract " + files[i] + " with " + len + " bytes to local directory...");
          IndexInput ii = cfr.openInput(files[i]);

          FileOutputStream f = new FileOutputStream(files[i]);

          // read and write with a small buffer, which is more effectiv than reading byte by byte
          byte[] buffer = new byte[1024];
          int chunk = buffer.length;
          while(len > 0) {
            final int bufLen = (int) Math.min(chunk, len);
            ii.readBytes(buffer, 0, bufLen);
            f.write(buffer, 0, bufLen);
            len -= bufLen;
          }

          f.close();
          ii.close();
        }
        else
          System.out.println(files[i] + ": " + len + " bytes");
      }
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
    finally {
      try {
        if (dir != null)
          dir.close();
        if (cfr != null)
          cfr.close();
      }
      catch (IOException ioe) {
        ioe.printStackTrace();
      }
    }
  }
}
