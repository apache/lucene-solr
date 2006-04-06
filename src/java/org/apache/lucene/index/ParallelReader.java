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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

/** An IndexReader which reads multiple, parallel indexes.  Each index added
 * must have the same number of documents, but typically each contains
 * different fields.  Each document contains the union of the fields of all
 * documents with the same document number.  When searching, matches for a
 * query term are from the first index added that has the field.
 *
 * <p>This is useful, e.g., with collections that have large fields which
 * change rarely and small fields that change more frequently.  The smaller
 * fields may be re-indexed in a new index and both indexes may be searched
 * together.
 * 
 * <p><strong>Warning:</strong> It is up to you to make sure all indexes
 * are created and modified the same way. For example, if you add
 * documents to one index, you need to add the same documents in the
 * same order to the other indexes. <em>Failure to do so will result in
 * undefined behavior</em>.
 */
public class ParallelReader extends IndexReader {
  private List readers = new ArrayList();
  private SortedMap fieldToReader = new TreeMap();
  private List storedFieldReaders = new ArrayList(); 

  private int maxDoc;
  private int numDocs;
  private boolean hasDeletions;

 /** Construct a ParallelReader. */
  public ParallelReader() throws IOException { super(null); }
    
 /** Add an IndexReader. */
  public void add(IndexReader reader) throws IOException {
    add(reader, false);
  }

 /** Add an IndexReader whose stored fields will not be returned.  This can
  * accellerate search when stored fields are only needed from a subset of
  * the IndexReaders.
  * 
  * @throws IllegalArgumentException if not all indexes contain the same number 
  *     of documents
  * @throws IllegalArgumentException if not all indexes have the same value 
  *     of {@link IndexReader#maxDoc()}
  */
  public void add(IndexReader reader, boolean ignoreStoredFields)
    throws IOException {

    if (readers.size() == 0) {
      this.maxDoc = reader.maxDoc();
      this.numDocs = reader.numDocs();
      this.hasDeletions = reader.hasDeletions();
    }

    if (reader.maxDoc() != maxDoc)                // check compatibility
      throw new IllegalArgumentException
        ("All readers must have same maxDoc: "+maxDoc+"!="+reader.maxDoc());
    if (reader.numDocs() != numDocs)
      throw new IllegalArgumentException
        ("All readers must have same numDocs: "+numDocs+"!="+reader.numDocs());
    
    Iterator i = reader.getFieldNames(IndexReader.FieldOption.ALL).iterator();
    while (i.hasNext()) {                         // update fieldToReader map
      String field = (String)i.next();
      if (fieldToReader.get(field) == null)
        fieldToReader.put(field, reader);
    }

    if (!ignoreStoredFields)
      storedFieldReaders.add(reader);             // add to storedFieldReaders
    readers.add(reader);
  }

  public int numDocs() { return numDocs; }

  public int maxDoc() { return maxDoc; }

  public boolean hasDeletions() { return hasDeletions; }

  // check first reader
  public boolean isDeleted(int n) {
    if (readers.size() > 0)
      return ((IndexReader)readers.get(0)).isDeleted(n);
    return false;
  }

  // delete in all readers
  protected void doDelete(int n) throws IOException {
    for (int i = 0; i < readers.size(); i++) {
      ((IndexReader)readers.get(i)).doDelete(n);
    }
    hasDeletions = true;
  }

  // undeleteAll in all readers
  protected void doUndeleteAll() throws IOException {
    for (int i = 0; i < readers.size(); i++) {
      ((IndexReader)readers.get(i)).doUndeleteAll();
    }
    hasDeletions = false;
  }

  // append fields from storedFieldReaders
  public Document document(int n) throws IOException {
    Document result = new Document();
    for (int i = 0; i < storedFieldReaders.size(); i++) {
      IndexReader reader = (IndexReader)storedFieldReaders.get(i);
      Enumeration fields = reader.document(n).fields();
      while (fields.hasMoreElements()) {
        result.add((Field)fields.nextElement());
      }
    }
    return result;
  }

  // get all vectors
  public TermFreqVector[] getTermFreqVectors(int n) throws IOException {
    ArrayList results = new ArrayList();
    Iterator i = fieldToReader.entrySet().iterator();
    while (i.hasNext()) {
      Map.Entry e = (Map.Entry)i.next();
      String field = (String)e.getKey();
      IndexReader reader = (IndexReader)e.getValue();
      TermFreqVector vector = reader.getTermFreqVector(n, field);
      if (vector != null)
        results.add(vector);
    }
    return (TermFreqVector[])
      results.toArray(new TermFreqVector[results.size()]);
  }

  public TermFreqVector getTermFreqVector(int n, String field)
    throws IOException {
    return ((IndexReader)fieldToReader.get(field)).getTermFreqVector(n, field);
  }

  public boolean hasNorms(String field) throws IOException {
    return ((IndexReader)fieldToReader.get(field)).hasNorms(field);
  }

  public byte[] norms(String field) throws IOException {
    return ((IndexReader)fieldToReader.get(field)).norms(field);
  }

  public void norms(String field, byte[] result, int offset)
    throws IOException {
     ((IndexReader)fieldToReader.get(field)).norms(field, result, offset);
  }

  protected void doSetNorm(int n, String field, byte value)
    throws IOException {
    ((IndexReader)fieldToReader.get(field)).doSetNorm(n, field, value);
  }

  public TermEnum terms() throws IOException {
    return new ParallelTermEnum();
  }

  public TermEnum terms(Term term) throws IOException {
    return new ParallelTermEnum(term);
  }

  public int docFreq(Term term) throws IOException {
    return ((IndexReader)fieldToReader.get(term.field())).docFreq(term);
  }

  public TermDocs termDocs(Term term) throws IOException {
    return new ParallelTermDocs(term);
  }

  public TermDocs termDocs() throws IOException {
    return new ParallelTermDocs();
  }

  public TermPositions termPositions(Term term) throws IOException {
    return new ParallelTermPositions(term);
  }

  public TermPositions termPositions() throws IOException {
    return new ParallelTermPositions();
  }

  protected void doCommit() throws IOException {
    for (int i = 0; i < readers.size(); i++)
      ((IndexReader)readers.get(i)).commit();
  }

  protected synchronized void doClose() throws IOException {
    for (int i = 0; i < readers.size(); i++)
      ((IndexReader)readers.get(i)).close();
  }


  public Collection getFieldNames (IndexReader.FieldOption fieldNames) {
    Set fieldSet = new HashSet();
    for (int i = 0; i < readers.size(); i++) {
      IndexReader reader = ((IndexReader)readers.get(i));
      Collection names = reader.getFieldNames(fieldNames);
      fieldSet.addAll(names);
    }
    return fieldSet;
  }

  private class ParallelTermEnum extends TermEnum {
    private String field;
    private TermEnum termEnum;

    public ParallelTermEnum() throws IOException {
      field = (String)fieldToReader.firstKey();
      if (field != null)
        termEnum = ((IndexReader)fieldToReader.get(field)).terms();
    }
    
    public ParallelTermEnum(Term term) throws IOException {
      field = term.field();
      termEnum = ((IndexReader)fieldToReader.get(field)).terms(term);
    }
    
    public boolean next() throws IOException {
      if (field == null)
        return false;

      boolean next = termEnum.next();

      // still within field?
      if (next && termEnum.term().field() == field)
        return true;                              // yes, keep going
      
      termEnum.close();                           // close old termEnum

      // find the next field, if any
      field = (String)fieldToReader.tailMap(field).firstKey();
      if (field != null) {
        termEnum = ((IndexReader)fieldToReader.get(field)).terms();
        return true;
      }

      return false;                               // no more fields
        
    }

    public Term term() { return termEnum.term(); }
    public int docFreq() { return termEnum.docFreq(); }
    public void close() throws IOException { termEnum.close(); }

  }

  // wrap a TermDocs in order to support seek(Term)
  private class ParallelTermDocs implements TermDocs {
    protected TermDocs termDocs;

    public ParallelTermDocs() {}
    public ParallelTermDocs(Term term) throws IOException { seek(term); }

    public int doc() { return termDocs.doc(); }
    public int freq() { return termDocs.freq(); }

    public void seek(Term term) throws IOException {
      termDocs = ((IndexReader)fieldToReader.get(term.field())).termDocs(term);
    }

    public void seek(TermEnum termEnum) throws IOException {
      seek(termEnum.term());
    }

    public boolean next() throws IOException { return termDocs.next(); }

    public int read(final int[] docs, final int[] freqs) throws IOException {
      return termDocs.read(docs, freqs);
    }

    public boolean skipTo(int target) throws IOException {
      return termDocs.skipTo(target);
    }

    public void close() throws IOException { termDocs.close(); }

  }

  private class ParallelTermPositions
    extends ParallelTermDocs implements TermPositions {

    public ParallelTermPositions() {}
    public ParallelTermPositions(Term term) throws IOException { seek(term); }

    public void seek(Term term) throws IOException {
      termDocs = ((IndexReader)fieldToReader.get(term.field()))
        .termPositions(term);
    }

    public int nextPosition() throws IOException {
      return ((TermPositions)termDocs).nextPosition();
    }

  }

}

