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
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.FieldSelectorResult;

import java.io.IOException;
import java.util.SortedMap;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.Enumeration;
import java.util.Set;
import java.util.HashSet;


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
  private Map readerToFields = new HashMap();
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

    Collection fields = reader.getFieldNames(IndexReader.FieldOption.ALL);
    readerToFields.put(reader, fields);
    Iterator i = fields.iterator();
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
      ((IndexReader)readers.get(i)).deleteDocument(n);
    }
    hasDeletions = true;
  }

  // undeleteAll in all readers
  protected void doUndeleteAll() throws IOException {
    for (int i = 0; i < readers.size(); i++) {
      ((IndexReader)readers.get(i)).undeleteAll();
    }
    hasDeletions = false;
  }

  // append fields from storedFieldReaders
  public Document document(int n, FieldSelector fieldSelector) throws IOException {
    Document result = new Document();
    for (int i = 0; i < storedFieldReaders.size(); i++) {
      IndexReader reader = (IndexReader)storedFieldReaders.get(i);

      boolean include = (fieldSelector==null);
      if (!include) {
        Iterator it = ((Collection) readerToFields.get(reader)).iterator();
        while (it.hasNext())
          if (fieldSelector.accept((String)it.next())!=FieldSelectorResult.NO_LOAD) {
            include = true;
            break;
          }
      }
      if (include) {
        Enumeration fields = reader.document(n, fieldSelector).fields();
        while (fields.hasMoreElements()) {
          result.add((Fieldable)fields.nextElement());
        }
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
    IndexReader reader = ((IndexReader)fieldToReader.get(field));
    return reader==null ? null : reader.getTermFreqVector(n, field);
  }

  public boolean hasNorms(String field) throws IOException {
    IndexReader reader = ((IndexReader)fieldToReader.get(field));
    return reader==null ? false : reader.hasNorms(field);
  }

  public byte[] norms(String field) throws IOException {
    IndexReader reader = ((IndexReader)fieldToReader.get(field));
    return reader==null ? null : reader.norms(field);
  }

  public void norms(String field, byte[] result, int offset)
    throws IOException {
    IndexReader reader = ((IndexReader)fieldToReader.get(field));
    if (reader!=null)
      reader.norms(field, result, offset);
  }

  protected void doSetNorm(int n, String field, byte value)
    throws IOException {
    IndexReader reader = ((IndexReader)fieldToReader.get(field));
    if (reader!=null)
      reader.doSetNorm(n, field, value);
  }

  public TermEnum terms() throws IOException {
    return new ParallelTermEnum();
  }

  public TermEnum terms(Term term) throws IOException {
    return new ParallelTermEnum(term);
  }

  public int docFreq(Term term) throws IOException {
    IndexReader reader = ((IndexReader)fieldToReader.get(term.field()));
    return reader==null ? 0 : reader.docFreq(term);
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
    private Iterator fieldIterator;
    private TermEnum termEnum;

    public ParallelTermEnum() throws IOException {
      field = (String)fieldToReader.firstKey();
      if (field != null)
        termEnum = ((IndexReader)fieldToReader.get(field)).terms();
    }

    public ParallelTermEnum(Term term) throws IOException {
      field = term.field();
      IndexReader reader = ((IndexReader)fieldToReader.get(field));
      if (reader!=null)
        termEnum = reader.terms(term);
    }

    public boolean next() throws IOException {
      if (termEnum==null)
        return false;

      // another term in this field?
      if (termEnum.next() && termEnum.term().field()==field)
        return true;                              // yes, keep going

      termEnum.close();                           // close old termEnum

      // find the next field with terms, if any
      if (fieldIterator==null) {
        fieldIterator = fieldToReader.tailMap(field).keySet().iterator();
        fieldIterator.next();                     // Skip field to get next one
      }
      while (fieldIterator.hasNext()) {
        field = (String) fieldIterator.next();
        termEnum = ((IndexReader)fieldToReader.get(field)).terms(new Term(field, ""));
        Term term = termEnum.term();
        if (term!=null && term.field()==field)
          return true;
        else
          termEnum.close();
      }
 
      return false;                               // no more fields
    }

    public Term term() {
      if (termEnum==null)
        return null;

      return termEnum.term();
    }

    public int docFreq() {
      if (termEnum==null)
        return 0;

      return termEnum.docFreq();
    }

    public void close() throws IOException {
      if (termEnum!=null)
        termEnum.close();
    }

  }

  // wrap a TermDocs in order to support seek(Term)
  private class ParallelTermDocs implements TermDocs {
    protected TermDocs termDocs;

    public ParallelTermDocs() {}
    public ParallelTermDocs(Term term) throws IOException { seek(term); }

    public int doc() { return termDocs.doc(); }
    public int freq() { return termDocs.freq(); }

    public void seek(Term term) throws IOException {
      IndexReader reader = ((IndexReader)fieldToReader.get(term.field()));
      termDocs = reader!=null ? reader.termDocs(term) : null;
    }

    public void seek(TermEnum termEnum) throws IOException {
      seek(termEnum.term());
    }

    public boolean next() throws IOException {
      if (termDocs==null)
        return false;

      return termDocs.next();
    }

    public int read(final int[] docs, final int[] freqs) throws IOException {
      if (termDocs==null)
        return 0;

      return termDocs.read(docs, freqs);
    }

    public boolean skipTo(int target) throws IOException {
      if (termDocs==null)
        return false;

      return termDocs.skipTo(target);
    }

    public void close() throws IOException {
      if (termDocs!=null)
        termDocs.close();
    }

  }

  private class ParallelTermPositions
    extends ParallelTermDocs implements TermPositions {

    public ParallelTermPositions() {}
    public ParallelTermPositions(Term term) throws IOException { seek(term); }

    public void seek(Term term) throws IOException {
      IndexReader reader = ((IndexReader)fieldToReader.get(term.field()));
      termDocs = reader!=null ? reader.termPositions(term) : null;
    }

    public int nextPosition() throws IOException {
      // It is an error to call this if there is no next position, e.g. if termDocs==null
      return ((TermPositions)termDocs).nextPosition();
    }

  }

}

