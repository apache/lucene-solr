package org.apache.lucene.index;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
 * reserved.
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
import java.io.Reader;
import java.io.StringReader;
import java.util.Hashtable;
import java.util.Enumeration;

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
  private FieldInfos fieldInfos;
  private int maxFieldLength;
  
  DocumentWriter(Directory d, Analyzer a, int mfl) {
    directory = d;
    analyzer = a;
    maxFieldLength = mfl;
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
    fieldLengths = new int[fieldInfos.size()];	  // init fieldLengths
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

  // Tokenizes the fields of a document into Postings.
  private final void invertDocument(Document doc)
       throws IOException {
    Enumeration fields  = doc.fields();
    while (fields.hasMoreElements()) {
      Field field = (Field)fields.nextElement();
      String fieldName = field.name();
      int fieldNumber = fieldInfos.fieldNumber(fieldName);

      int position = fieldLengths[fieldNumber];	  // position in field

      if (field.isIndexed()) {
	if (!field.isTokenized()) {		  // un-tokenized field
	  addPosition(fieldName, field.stringValue(), position++);
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
	      addPosition(fieldName, t.termText(), position++);
	      if (position > maxFieldLength) break;
	    }
	  } finally {
	    stream.close();
	  }
	}

	fieldLengths[fieldNumber] = position;	  // save field length
      }
    }
  }

  private final Term termBuffer = new Term("", ""); // avoid consing

  private final void addPosition(String field, String text, int position) {
    termBuffer.set(field, text);
    Posting ti = (Posting)postingTable.get(termBuffer);
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
    }
    else {					  // word not seen before
      Term term = new Term(field, text, false);
      postingTable.put(term, new Posting(term, position));
    }
  }

  private final Posting[] sortPostingTable() {
    // copy postingTable into an array
    Posting[] array = new Posting[postingTable.size()];
    Enumeration postings = postingTable.elements();
    for (int i = 0; postings.hasMoreElements(); i++)
      array[i] = (Posting)postings.nextElement();

    // sort the array
    quickSort(array, 0, array.length - 1);

    return array;
  }

  static private final void quickSort(Posting[] postings, int lo, int hi) {
    if(lo >= hi)
      return;

    int mid = (lo + hi) / 2;

    if(postings[lo].term.compareTo(postings[mid].term) > 0) {
      Posting tmp = postings[lo];
      postings[lo] = postings[mid];
      postings[mid] = tmp;
    }

    if(postings[mid].term.compareTo(postings[hi].term) > 0) {
      Posting tmp = postings[mid];
      postings[mid] = postings[hi];
      postings[hi] = tmp;
      
      if(postings[lo].term.compareTo(postings[mid].term) > 0) {
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
    
    for( ;; ) {
      while(postings[right].term.compareTo(partition) > 0)
	--right;
      
      while(left < right && postings[left].term.compareTo(partition) <= 0)
	++left;
      
      if(left < right) {
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

    try {
      freq = directory.createFile(segment + ".frq");
      prox = directory.createFile(segment + ".prx");
      tis = new TermInfosWriter(directory, segment, fieldInfos);
      TermInfo ti = new TermInfo();

      for (int i = 0; i < postings.length; i++) {
	Posting posting = postings[i];

	// add an entry to the dictionary with pointers to prox and freq files
	ti.set(1, freq.getFilePointer(), prox.getFilePointer());
	tis.add(posting.term, ti);
	
	// add an entry to the freq file
	int f = posting.freq;
	if (f == 1)				  // optimize freq=1
	  freq.writeVInt(1);			  // set low bit of doc num.
	else {
	  freq.writeVInt(0);			  // the document number
	  freq.writeVInt(f);			  // frequency in doc
	}
	
	int lastPosition = 0;			  // write positions
	int[] positions = posting.positions;
	for (int j = 0; j < f; j++) {		  // use delta-encoding
	  int position = positions[j];
	  prox.writeVInt(position - lastPosition);
	  lastPosition = position;
	}
      }
    }
    finally {
      if (freq != null) freq.close();
      if (prox != null) prox.close();
      if (tis  != null)  tis.close();
    }
  }

  private final void writeNorms(Document doc, String segment)
       throws IOException {
    Enumeration fields  = doc.fields();
    while (fields.hasMoreElements()) {
      Field field = (Field)fields.nextElement();
      if (field.isIndexed()) {
	int fieldNumber = fieldInfos.fieldNumber(field.name());
	OutputStream norm = directory.createFile(segment + ".f" + fieldNumber);
	try {
	  norm.writeByte(Similarity.norm(fieldLengths[fieldNumber]));
	} finally {
	  norm.close();
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
