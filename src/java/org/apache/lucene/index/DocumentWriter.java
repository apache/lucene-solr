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
import java.io.Reader;
import java.io.StringReader;
import java.util.Hashtable;
import java.util.Enumeration;
import java.util.Arrays;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.OutputStream;
import org.apache.lucene.search.Similarity;

final class DocumentWriter {
  private Analyzer analyzer;
  private Directory directory;
  private Similarity similarity;
  private FieldInfos fieldInfos;
  private int maxFieldLength;

  /**
   * 
   * @param directory The directory to write the document information to
   * @param analyzer The analyzer to use for the document
   * @param similarity The Similarity function
   * @param maxFieldLength The maximum number of tokens a field may have
   */ 
  DocumentWriter(Directory directory, Analyzer analyzer,
                 Similarity similarity, int maxFieldLength) {
    this.directory = directory;
    this.analyzer = analyzer;
    this.similarity = similarity;
    this.maxFieldLength = maxFieldLength;
  }

  final void addDocument(String segment, Document doc)
          throws IOException {
    // write field names
    fieldInfos = new FieldInfos();
    fieldInfos.add(doc);
    fieldInfos.write(directory, segment + ".fnm");

    // write field values
    FieldsWriter fieldsWriter =
            new FieldsWriter(directory, segment, fieldInfos);
    try {
      fieldsWriter.addDocument(doc);
    } finally {
      fieldsWriter.close();
    }

    // invert doc into postingTable
    postingTable.clear();			  // clear postingTable
    fieldLengths = new int[fieldInfos.size()];    // init fieldLengths
    fieldPositions = new int[fieldInfos.size()];  // init fieldPositions

    fieldBoosts = new float[fieldInfos.size()];	  // init fieldBoosts
    Arrays.fill(fieldBoosts, doc.getBoost());

    invertDocument(doc);

    // sort postingTable into an array
    Posting[] postings = sortPostingTable();

    /*
    for (int i = 0; i < postings.length; i++) {
      Posting posting = postings[i];
      System.out.print(posting.term);
      System.out.print(" freq=" + posting.freq);
      System.out.print(" pos=");
      System.out.print(posting.positions[0]);
      for (int j = 1; j < posting.freq; j++)
	System.out.print("," + posting.positions[j]);
      System.out.println("");
    }
    */

    // write postings
    writePostings(postings, segment);

    // write norms of indexed fields
    writeNorms(doc, segment);

  }

  // Keys are Terms, values are Postings.
  // Used to buffer a document before it is written to the index.
  private final Hashtable postingTable = new Hashtable();
  private int[] fieldLengths;
  private int[] fieldPositions;
  private float[] fieldBoosts;

  // Tokenizes the fields of a document into Postings.
  private final void invertDocument(Document doc)
          throws IOException {
    Enumeration fields = doc.fields();
    while (fields.hasMoreElements()) {
      Field field = (Field) fields.nextElement();
      String fieldName = field.name();
      int fieldNumber = fieldInfos.fieldNumber(fieldName);

      int length = fieldLengths[fieldNumber];     // length of field
      int position = fieldPositions[fieldNumber]; // position in field

      if (field.isIndexed()) {
        if (!field.isTokenized()) {		  // un-tokenized field
          addPosition(fieldName, field.stringValue(), position++);
          length++;
        } else {
          Reader reader;			  // find or make Reader
          if (field.readerValue() != null)
            reader = field.readerValue();
          else if (field.stringValue() != null)
            reader = new StringReader(field.stringValue());
          else
            throw new IllegalArgumentException
                    ("field must have either String or Reader value");

          // Tokenize field and add to postingTable
          TokenStream stream = analyzer.tokenStream(fieldName, reader);
          try {
            for (Token t = stream.next(); t != null; t = stream.next()) {
              position += (t.getPositionIncrement() - 1);
              addPosition(fieldName, t.termText(), position++);
              if (++length > maxFieldLength) break;
            }
          } finally {
            stream.close();
          }
        }

        fieldLengths[fieldNumber] = length;	  // save field length
        fieldPositions[fieldNumber] = position;	  // save field position
        fieldBoosts[fieldNumber] *= field.getBoost();
      }
    }
  }

  private final Term termBuffer = new Term("", ""); // avoid consing

  private final void addPosition(String field, String text, int position) {
    termBuffer.set(field, text);
    Posting ti = (Posting) postingTable.get(termBuffer);
    if (ti != null) {				  // word seen before
      int freq = ti.freq;
      if (ti.positions.length == freq) {	  // positions array is full
        int[] newPositions = new int[freq * 2];	  // double size
        int[] positions = ti.positions;
        for (int i = 0; i < freq; i++)		  // copy old positions to new
          newPositions[i] = positions[i];
        ti.positions = newPositions;
      }
      ti.positions[freq] = position;		  // add new position
      ti.freq = freq + 1;			  // update frequency
    } else {					  // word not seen before
      Term term = new Term(field, text, false);
      postingTable.put(term, new Posting(term, position));
    }
  }

  private final Posting[] sortPostingTable() {
    // copy postingTable into an array
    Posting[] array = new Posting[postingTable.size()];
    Enumeration postings = postingTable.elements();
    for (int i = 0; postings.hasMoreElements(); i++)
      array[i] = (Posting) postings.nextElement();

    // sort the array
    quickSort(array, 0, array.length - 1);

    return array;
  }

  private static final void quickSort(Posting[] postings, int lo, int hi) {
    if (lo >= hi)
      return;

    int mid = (lo + hi) / 2;

    if (postings[lo].term.compareTo(postings[mid].term) > 0) {
      Posting tmp = postings[lo];
      postings[lo] = postings[mid];
      postings[mid] = tmp;
    }

    if (postings[mid].term.compareTo(postings[hi].term) > 0) {
      Posting tmp = postings[mid];
      postings[mid] = postings[hi];
      postings[hi] = tmp;

      if (postings[lo].term.compareTo(postings[mid].term) > 0) {
        Posting tmp2 = postings[lo];
        postings[lo] = postings[mid];
        postings[mid] = tmp2;
      }
    }

    int left = lo + 1;
    int right = hi - 1;

    if (left >= right)
      return;

    Term partition = postings[mid].term;

    for (; ;) {
      while (postings[right].term.compareTo(partition) > 0)
        --right;

      while (left < right && postings[left].term.compareTo(partition) <= 0)
        ++left;

      if (left < right) {
        Posting tmp = postings[left];
        postings[left] = postings[right];
        postings[right] = tmp;
        --right;
      } else {
        break;
      }
    }

    quickSort(postings, lo, left);
    quickSort(postings, left + 1, hi);
  }

  private final void writePostings(Posting[] postings, String segment)
          throws IOException {
    OutputStream freq = null, prox = null;
    TermInfosWriter tis = null;
    TermVectorsWriter termVectorWriter = null;
    try {
      //open files for inverse index storage
      freq = directory.createFile(segment + ".frq");
      prox = directory.createFile(segment + ".prx");
      tis = new TermInfosWriter(directory, segment, fieldInfos);
      TermInfo ti = new TermInfo();
      String currentField = null;

      for (int i = 0; i < postings.length; i++) {
        Posting posting = postings[i];

        // add an entry to the dictionary with pointers to prox and freq files
        ti.set(1, freq.getFilePointer(), prox.getFilePointer(), -1);
        tis.add(posting.term, ti);

        // add an entry to the freq file
        int postingFreq = posting.freq;
        if (postingFreq == 1)				  // optimize freq=1
          freq.writeVInt(1);			  // set low bit of doc num.
        else {
          freq.writeVInt(0);			  // the document number
          freq.writeVInt(postingFreq);			  // frequency in doc
        }

        int lastPosition = 0;			  // write positions
        int[] positions = posting.positions;
        for (int j = 0; j < postingFreq; j++) {		  // use delta-encoding
          int position = positions[j];
          prox.writeVInt(position - lastPosition);
          lastPosition = position;
        }
        // check to see if we switched to a new field
        String termField = posting.term.field();
        if (currentField != termField) {
          // changing field - see if there is something to save
          currentField = termField;
          FieldInfo fi = fieldInfos.fieldInfo(currentField);
          if (fi.storeTermVector) {
            if (termVectorWriter == null) {
              termVectorWriter =
                new TermVectorsWriter(directory, segment, fieldInfos);
              termVectorWriter.openDocument();
            }
            termVectorWriter.openField(currentField);
          } else if (termVectorWriter != null) {
            termVectorWriter.closeField();
          }
        }
        if (termVectorWriter != null && termVectorWriter.isFieldOpen()) {
          termVectorWriter.addTerm(posting.term.text(), postingFreq);
        }
      }
      if (termVectorWriter != null)
        termVectorWriter.closeDocument();
    } finally {
      // make an effort to close all streams we can but remember and re-throw
      // the first exception encountered in this process
      IOException keep = null;
      if (freq != null) try { freq.close(); } catch (IOException e) { if (keep == null) keep = e; }
      if (prox != null) try { prox.close(); } catch (IOException e) { if (keep == null) keep = e; }
      if (tis  != null) try {  tis.close(); } catch (IOException e) { if (keep == null) keep = e; }
      if (termVectorWriter  != null) try {  termVectorWriter.close(); } catch (IOException e) { if (keep == null) keep = e; }
      if (keep != null) throw (IOException) keep.fillInStackTrace();
    }
  }

  private final void writeNorms(Document doc, String segment) throws IOException { 
    for(int n = 0; n < fieldInfos.size(); n++){
      FieldInfo fi = fieldInfos.fieldInfo(n);
      if(fi.isIndexed){
        float norm = fieldBoosts[n] * similarity.lengthNorm(fi.name, fieldLengths[n]);
        OutputStream norms = directory.createFile(segment + ".f" + n);
        try {
          norms.writeByte(Similarity.encodeNorm(norm));
        } finally {
          norms.close();
        }
      }
    }
  }
}

final class Posting {				  // info about a Term in a doc
  Term term;					  // the Term
  int freq;					  // its frequency in doc
  int[] positions;				  // positions it occurs at

  Posting(Term t, int position) {
    term = t;
    freq = 1;
    positions = new int[1];
    positions[0] = position;
  }
}
