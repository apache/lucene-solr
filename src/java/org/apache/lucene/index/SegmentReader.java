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

import java.io.IOException;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

import org.apache.lucene.document.Document;
import org.apache.lucene.store.InputStream;
import org.apache.lucene.store.OutputStream;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BitVector;

/**
 * FIXME: Describe class <code>SegmentReader</code> here.
 *
 * @version $Id$
 */
final class SegmentReader extends IndexReader {
  private String segment;

  FieldInfos fieldInfos;
  private FieldsReader fieldsReader;

  TermInfosReader tis;
  TermVectorsReader termVectorsReader;

  BitVector deletedDocs = null;
  private boolean deletedDocsDirty = false;
  private boolean normsDirty = false;
  private boolean undeleteAll = false;

  InputStream freqStream;
  InputStream proxStream;

  // Compound File Reader when based on a compound file segment
  CompoundFileReader cfsReader = null;

  private class Norm {
    public Norm(InputStream in, int number) 
    { 
      this.in = in; 
      this.number = number;
    }

    private InputStream in;
    private byte[] bytes;
    private boolean dirty;
    private int number;

    private void reWrite() throws IOException {
      // NOTE: norms are re-written in regular directory, not cfs
      OutputStream out = directory().createFile(segment + ".tmp");
      try {
        out.writeBytes(bytes, maxDoc());
      } finally {
        out.close();
      }
      String fileName;
      if(cfsReader == null)
          fileName = segment + ".f" + number;
      else{ 
          // use a different file name if we have compound format
          fileName = segment + ".s" + number;
      }
      directory().renameFile(segment + ".tmp", fileName);
      this.dirty = false;
    }
  }

  private Hashtable norms = new Hashtable();

  SegmentReader(SegmentInfos sis, SegmentInfo si, boolean closeDir)
          throws IOException {
    super(si.dir, sis, closeDir);
    initialize(si);
  }

  SegmentReader(SegmentInfo si) throws IOException {
    super(si.dir);
    initialize(si);
  }
          
   private void initialize(SegmentInfo si) throws IOException
   {
    segment = si.name;

    // Use compound file directory for some files, if it exists
    Directory cfsDir = directory();
    if (directory().fileExists(segment + ".cfs")) {
      cfsReader = new CompoundFileReader(directory(), segment + ".cfs");
      cfsDir = cfsReader;
    }

    // No compound file exists - use the multi-file format
    fieldInfos = new FieldInfos(cfsDir, segment + ".fnm");
    fieldsReader = new FieldsReader(cfsDir, segment, fieldInfos);

    tis = new TermInfosReader(cfsDir, segment, fieldInfos);

    // NOTE: the bitvector is stored using the regular directory, not cfs
    if (hasDeletions(si))
      deletedDocs = new BitVector(directory(), segment + ".del");

    // make sure that all index files have been read or are kept open
    // so that if an index update removes them we'll still have them
    freqStream = cfsDir.openFile(segment + ".frq");
    proxStream = cfsDir.openFile(segment + ".prx");
    openNorms(cfsDir);

    if (fieldInfos.hasVectors()) { // open term vector files only as needed
      termVectorsReader = new TermVectorsReader(cfsDir, segment, fieldInfos);
    }
  }

  protected final void doCommit() throws IOException {
    if (deletedDocsDirty) {               // re-write deleted 
      deletedDocs.write(directory(), segment + ".tmp");
      directory().renameFile(segment + ".tmp", segment + ".del");
    }
    if(undeleteAll && directory().fileExists(segment + ".del")){
      directory().deleteFile(segment + ".del");
    }
    if (normsDirty) {               // re-write norms 
      Enumeration values = norms.elements();
      while (values.hasMoreElements()) {
        Norm norm = (Norm) values.nextElement();
        if (norm.dirty) {
          norm.reWrite();
        }
      }
    }
    deletedDocsDirty = false;
    normsDirty = false;
    undeleteAll = false;
  }
  
  protected final void doClose() throws IOException {
    fieldsReader.close();
    tis.close();

    if (freqStream != null)
      freqStream.close();
    if (proxStream != null)
      proxStream.close();

    closeNorms();
    if (termVectorsReader != null) termVectorsReader.close();

    if (cfsReader != null)
      cfsReader.close();
  }

  static final boolean hasDeletions(SegmentInfo si) throws IOException {
    return si.dir.fileExists(si.name + ".del");
  }

  public boolean hasDeletions() {
    return deletedDocs != null;
  }


  static final boolean usesCompoundFile(SegmentInfo si) throws IOException {
    return si.dir.fileExists(si.name + ".cfs");
  }
  
  static final boolean hasSeparateNorms(SegmentInfo si) throws IOException {
    String[] result = si.dir.list();
    String pattern = si.name + ".f";
    int patternLength = pattern.length();
    for(int i = 0; i < 0; i++){
      if(result[i].startsWith(pattern) && Character.isDigit(result[i].charAt(patternLength)))
        return true;
    }
    return false;
  }

  protected final void doDelete(int docNum) {
    if (deletedDocs == null)
      deletedDocs = new BitVector(maxDoc());
    deletedDocsDirty = true;
    undeleteAll = false;
    deletedDocs.set(docNum);
  }

  protected final void doUndeleteAll() {
      deletedDocs = null;
      deletedDocsDirty = false;
      undeleteAll = true;
  }

  final Vector files() throws IOException {
    Vector files = new Vector(16);
    final String ext[] = new String[]{
      "cfs", "fnm", "fdx", "fdt", "tii", "tis", "frq", "prx", "del",
      "tvx", "tvd", "tvf", "tvp" };

    for (int i = 0; i < ext.length; i++) {
      String name = segment + "." + ext[i];
      if (directory().fileExists(name))
        files.addElement(name);
    }

    for (int i = 0; i < fieldInfos.size(); i++) {
      FieldInfo fi = fieldInfos.fieldInfo(i);
      if (fi.isIndexed){
        String name;
        if(cfsReader == null)
            name = segment + ".f" + i;
        else
            name = segment + ".s" + i;
        if (directory().fileExists(name))
            files.addElement(name);
      }
    }
    return files;
  }

  public final TermEnum terms() {
    return tis.terms();
  }

  public final TermEnum terms(Term t) throws IOException {
    return tis.terms(t);
  }

  public final synchronized Document document(int n) throws IOException {
    if (isDeleted(n))
      throw new IllegalArgumentException
              ("attempt to access a deleted document");
    return fieldsReader.doc(n);
  }

  public final synchronized boolean isDeleted(int n) {
    return (deletedDocs != null && deletedDocs.get(n));
  }

  public final TermDocs termDocs() throws IOException {
    return new SegmentTermDocs(this);
  }

  public final TermPositions termPositions() throws IOException {
    return new SegmentTermPositions(this);
  }

  public final int docFreq(Term t) throws IOException {
    TermInfo ti = tis.get(t);
    if (ti != null)
      return ti.docFreq;
    else
      return 0;
  }

  public final int numDocs() {
    int n = maxDoc();
    if (deletedDocs != null)
      n -= deletedDocs.count();
    return n;
  }

  public final int maxDoc() {
    return fieldsReader.size();
  }

  /**
   * @see IndexReader#getFieldNames()
   */
  public Collection getFieldNames() {
    // maintain a unique set of field names
    Set fieldSet = new HashSet();
    for (int i = 0; i < fieldInfos.size(); i++) {
      FieldInfo fi = fieldInfos.fieldInfo(i);
      fieldSet.add(fi.name);
    }
    return fieldSet;
  }

  /**
   * @see IndexReader#getFieldNames(boolean)
   */
  public Collection getFieldNames(boolean indexed) {
    // maintain a unique set of field names
    Set fieldSet = new HashSet();
    for (int i = 0; i < fieldInfos.size(); i++) {
      FieldInfo fi = fieldInfos.fieldInfo(i);
      if (fi.isIndexed == indexed)
        fieldSet.add(fi.name);
    }
    return fieldSet;
  }

  /**
   * 
   * @param storedTermVector if true, returns only Indexed fields that have term vector info, 
   *                        else only indexed fields without term vector info 
   * @return Collection of Strings indicating the names of the fields
   */
  public Collection getIndexedFieldNames(boolean storedTermVector) {
    // maintain a unique set of field names
    Set fieldSet = new HashSet();
    for (int i = 0; i < fieldInfos.size(); i++) {
      FieldInfo fi = fieldInfos.fieldInfo(i);
      if (fi.isIndexed == true && fi.storeTermVector == storedTermVector){
        fieldSet.add(fi.name);
      }
    }
    return fieldSet;

  }

  public synchronized byte[] norms(String field) throws IOException {
    Norm norm = (Norm) norms.get(field);
    if (norm == null)                             // not an indexed field
      return null;
    if (norm.bytes == null) {                     // value not yet read
      byte[] bytes = new byte[maxDoc()];
      norms(field, bytes, 0);
      norm.bytes = bytes;                         // cache it
    }
    return norm.bytes;
  }

  protected final void doSetNorm(int doc, String field, byte value)
          throws IOException {
    Norm norm = (Norm) norms.get(field);
    if (norm == null)                             // not an indexed field
      return;
    norm.dirty = true;                            // mark it dirty
    normsDirty = true;

    norms(field)[doc] = value;                    // set the value
  }

  /** Read norms into a pre-allocated array. */
  public synchronized void norms(String field, byte[] bytes, int offset)
    throws IOException {

    Norm norm = (Norm) norms.get(field);
    if (norm == null)
      return;					  // use zeros in array

    if (norm.bytes != null) {                     // can copy from cache
      System.arraycopy(norm.bytes, 0, bytes, offset, maxDoc());
      return;
    }

    InputStream normStream = (InputStream) norm.in.clone();
    try {                                         // read from disk
      normStream.seek(0);
      normStream.readBytes(bytes, offset, maxDoc());
    } finally {
      normStream.close();
    }
  }

  private final void openNorms(Directory cfsDir) throws IOException {
    for (int i = 0; i < fieldInfos.size(); i++) {
      FieldInfo fi = fieldInfos.fieldInfo(i);
      if (fi.isIndexed) {
        // look first if there are separate norms in compound format
        String fileName = segment + ".s" + fi.number;
        Directory d = directory();
        if(!d.fileExists(fileName)){
            fileName = segment + ".f" + fi.number;
            d = cfsDir;
        }
        norms.put(fi.name, new Norm(d.openFile(fileName), fi.number));
      }
    }
  }

  private final void closeNorms() throws IOException {
    synchronized (norms) {
      Enumeration enumerator = norms.elements();
      while (enumerator.hasMoreElements()) {
        Norm norm = (Norm) enumerator.nextElement();
        norm.in.close();
      }
    }
  }
  
  /** Return a term frequency vector for the specified document and field. The
   *  vector returned contains term numbers and frequencies for all terms in
   *  the specified field of this document, if the field had storeTermVector
   *  flag set.  If the flag was not set, the method returns null.
   */
  public TermFreqVector getTermFreqVector(int docNumber, String field) {
    // Check if this field is invalid or has no stored term vector
    FieldInfo fi = fieldInfos.fieldInfo(field);
    if (fi == null || !fi.storeTermVector) return null;

    return termVectorsReader.get(docNumber, field);
  }


  /** Return an array of term frequency vectors for the specified document.
   *  The array contains a vector for each vectorized field in the document.
   *  Each vector vector contains term numbers and frequencies for all terms
   *  in a given vectorized field.
   *  If no such fields existed, the method returns null.
   */
  public TermFreqVector[] getTermFreqVectors(int docNumber) {
    if (termVectorsReader == null)
      return null;

    return termVectorsReader.get(docNumber);
  }
}
