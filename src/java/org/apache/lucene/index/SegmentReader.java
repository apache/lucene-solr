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
import org.apache.lucene.document.Field;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BitVector;

/**
 * FIXME: Describe class <code>SegmentReader</code> here.
 *
 * @version $Id$
 */
class SegmentReader extends IndexReader {
  private String segment;

  FieldInfos fieldInfos;
  private FieldsReader fieldsReader;

  TermInfosReader tis;
  TermVectorsReader termVectorsReaderOrig = null;
  ThreadLocal termVectorsLocal = new ThreadLocal();

  BitVector deletedDocs = null;
  private boolean deletedDocsDirty = false;
  private boolean normsDirty = false;
  private boolean undeleteAll = false;

  IndexInput freqStream;
  IndexInput proxStream;

  // Compound File Reader when based on a compound file segment
  CompoundFileReader cfsReader = null;

  private class Norm {
    public Norm(IndexInput in, int number) 
    { 
      this.in = in; 
      this.number = number;
    }

    private IndexInput in;
    private byte[] bytes;
    private boolean dirty;
    private int number;

    private void reWrite() throws IOException {
      // NOTE: norms are re-written in regular directory, not cfs
      IndexOutput out = directory().createOutput(segment + ".tmp");
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

  /** The class which implements SegmentReader. */
  private static final Class IMPL;
  static {
    try {
      String name =
        System.getProperty("org.apache.lucene.SegmentReader.class",
                           SegmentReader.class.getName());
      IMPL = Class.forName(name);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("cannot load SegmentReader class: " + e.toString());
    }
  }

  protected SegmentReader() { super(null); }

  public static SegmentReader get(SegmentInfo si) throws IOException {
    return get(si.dir, si, null, false, false);
  }

  public static SegmentReader get(SegmentInfos sis, SegmentInfo si,
                                  boolean closeDir) throws IOException {
    return get(si.dir, si, sis, closeDir, true);
  }

  public static SegmentReader get(Directory dir, SegmentInfo si,
                                  SegmentInfos sis,
                                  boolean closeDir, boolean ownDir)
    throws IOException {
    SegmentReader instance;
    try {
      instance = (SegmentReader)IMPL.newInstance();
    } catch (Exception e) {
      throw new RuntimeException("cannot load SegmentReader class: " + e.toString());
    }
    instance.init(dir, sis, closeDir, ownDir);
    instance.initialize(si);
    return instance;
  }
          
   private void initialize(SegmentInfo si) throws IOException {
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
    freqStream = cfsDir.openInput(segment + ".frq");
    proxStream = cfsDir.openInput(segment + ".prx");
    openNorms(cfsDir);

    if (fieldInfos.hasVectors()) { // open term vector files only as needed
      termVectorsReaderOrig = new TermVectorsReader(cfsDir, segment, fieldInfos);
    }
  }
   
   protected void finalize() {
     // patch for pre-1.4.2 JVMs, whose ThreadLocals leak
     termVectorsLocal.set(null);
     super.finalize();
   }

  protected void doCommit() throws IOException {
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
  
  protected void doClose() throws IOException {
    fieldsReader.close();
    tis.close();

    if (freqStream != null)
      freqStream.close();
    if (proxStream != null)
      proxStream.close();

    closeNorms();
    
    if (termVectorsReaderOrig != null) 
      termVectorsReaderOrig.close();

    if (cfsReader != null)
      cfsReader.close();
  }

  static boolean hasDeletions(SegmentInfo si) throws IOException {
    return si.dir.fileExists(si.name + ".del");
  }

  public boolean hasDeletions() {
    return deletedDocs != null;
  }


  static boolean usesCompoundFile(SegmentInfo si) throws IOException {
    return si.dir.fileExists(si.name + ".cfs");
  }
  
  static boolean hasSeparateNorms(SegmentInfo si) throws IOException {
    String[] result = si.dir.list();
    String pattern = si.name + ".s";
    int patternLength = pattern.length();
    for(int i = 0; i < result.length; i++){
      if(result[i].startsWith(pattern) && Character.isDigit(result[i].charAt(patternLength)))
        return true;
    }
    return false;
  }

  protected void doDelete(int docNum) {
    if (deletedDocs == null)
      deletedDocs = new BitVector(maxDoc());
    deletedDocsDirty = true;
    undeleteAll = false;
    deletedDocs.set(docNum);
  }

  protected void doUndeleteAll() {
      deletedDocs = null;
      deletedDocsDirty = false;
      undeleteAll = true;
  }

  Vector files() throws IOException {
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

  public TermEnum terms() {
    return tis.terms();
  }

  public TermEnum terms(Term t) throws IOException {
    return tis.terms(t);
  }

  public synchronized Document document(int n) throws IOException {
    if (isDeleted(n))
      throw new IllegalArgumentException
              ("attempt to access a deleted document");
    return fieldsReader.doc(n);
  }

  public synchronized boolean isDeleted(int n) {
    return (deletedDocs != null && deletedDocs.get(n));
  }

  public TermDocs termDocs() throws IOException {
    return new SegmentTermDocs(this);
  }

  public TermPositions termPositions() throws IOException {
    return new SegmentTermPositions(this);
  }

  public int docFreq(Term t) throws IOException {
    TermInfo ti = tis.get(t);
    if (ti != null)
      return ti.docFreq;
    else
      return 0;
  }

  public int numDocs() {
    int n = maxDoc();
    if (deletedDocs != null)
      n -= deletedDocs.count();
    return n;
  }

  public int maxDoc() {
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
  
  public Collection getIndexedFieldNames (Field.TermVector tvSpec){
    boolean storedTermVector;
    boolean storePositionWithTermVector;
    boolean storeOffsetWithTermVector;
    
    if(tvSpec == Field.TermVector.NO){
      storedTermVector = false;
      storePositionWithTermVector = false;
      storeOffsetWithTermVector = false;
    }
    else if(tvSpec == Field.TermVector.YES){
      storedTermVector = true;
      storePositionWithTermVector = false;
      storeOffsetWithTermVector = false;
    }
    else if(tvSpec == Field.TermVector.WITH_POSITIONS){
      storedTermVector = true;
      storePositionWithTermVector = true;
      storeOffsetWithTermVector = false;
    }
    else if(tvSpec == Field.TermVector.WITH_OFFSETS){
      storedTermVector = true;
      storePositionWithTermVector = false;
      storeOffsetWithTermVector = true;
    }
    else if(tvSpec == Field.TermVector.WITH_POSITIONS_OFFSETS){
      storedTermVector = true;
      storePositionWithTermVector = true;
      storeOffsetWithTermVector = true;
    }
    else{
      throw new IllegalArgumentException("unknown termVector parameter " + tvSpec);
    }
    
    // maintain a unique set of field names
    Set fieldSet = new HashSet();
    for (int i = 0; i < fieldInfos.size(); i++) {
      FieldInfo fi = fieldInfos.fieldInfo(i);
      if (fi.isIndexed && fi.storeTermVector == storedTermVector && 
          fi.storePositionWithTermVector == storePositionWithTermVector && 
          fi.storeOffsetWithTermVector == storeOffsetWithTermVector){
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

  protected void doSetNorm(int doc, String field, byte value)
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

    IndexInput normStream = (IndexInput) norm.in.clone();
    try {                                         // read from disk
      normStream.seek(0);
      normStream.readBytes(bytes, offset, maxDoc());
    } finally {
      normStream.close();
    }
  }

  private void openNorms(Directory cfsDir) throws IOException {
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
        norms.put(fi.name, new Norm(d.openInput(fileName), fi.number));
      }
    }
  }

  private void closeNorms() throws IOException {
    synchronized (norms) {
      Enumeration enumerator = norms.elements();
      while (enumerator.hasMoreElements()) {
        Norm norm = (Norm) enumerator.nextElement();
        norm.in.close();
      }
    }
  }
  
  /**
   * Create a clone from the initial TermVectorsReader and store it in the ThreadLocal.
   * @return TermVectorsReader
   */
  private TermVectorsReader getTermVectorsReader() {
    TermVectorsReader tvReader = (TermVectorsReader)termVectorsLocal.get();
    if (tvReader == null) {
      tvReader = (TermVectorsReader)termVectorsReaderOrig.clone();
      termVectorsLocal.set(tvReader);
    }
    return tvReader;
  }
  
  /** Return a term frequency vector for the specified document and field. The
   *  vector returned contains term numbers and frequencies for all terms in
   *  the specified field of this document, if the field had storeTermVector
   *  flag set.  If the flag was not set, the method returns null.
   * @throws IOException
   */
  public TermFreqVector getTermFreqVector(int docNumber, String field) throws IOException {
    // Check if this field is invalid or has no stored term vector
    FieldInfo fi = fieldInfos.fieldInfo(field);
    if (fi == null || !fi.storeTermVector || termVectorsReaderOrig == null) 
      return null;
    
    TermVectorsReader termVectorsReader = getTermVectorsReader();
    return termVectorsReader.get(docNumber, field);
  }


  /** Return an array of term frequency vectors for the specified document.
   *  The array contains a vector for each vectorized field in the document.
   *  Each vector vector contains term numbers and frequencies for all terms
   *  in a given vectorized field.
   *  If no such fields existed, the method returns null.
   * @throws IOException
   */
  public TermFreqVector[] getTermFreqVectors(int docNumber) throws IOException {
    if (termVectorsReaderOrig == null)
      return null;
    
    TermVectorsReader termVectorsReader = getTermVectorsReader();
    return termVectorsReader.get(docNumber);
  }
}
