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

import java.util.Vector;
import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.OutputStream;
import org.apache.lucene.store.InputStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.BitVector;

final class SegmentMerger {
  private Directory directory;
  private String segment;

  private Vector readers = new Vector();
  private FieldInfos fieldInfos;
  
  SegmentMerger(Directory dir, String name) {
    directory = dir;
    segment = name;
  }

  final void add(SegmentReader reader) {
    readers.addElement(reader);
  }

  final SegmentReader segmentReader(int i) {
    return (SegmentReader)readers.elementAt(i);
  }

  final void merge() throws IOException {
    try {
      mergeFields();
      mergeTerms();
      mergeNorms();
      
    } finally {
      for (int i = 0; i < readers.size(); i++) {  // close readers
	SegmentReader reader = (SegmentReader)readers.elementAt(i);
	reader.close();
      }
    }
  }

  private final void mergeFields() throws IOException {
    fieldInfos = new FieldInfos();		  // merge field names
    for (int i = 0; i < readers.size(); i++) {
      SegmentReader reader = (SegmentReader)readers.elementAt(i);
      fieldInfos.add(reader.fieldInfos);
    }
    fieldInfos.write(directory, segment + ".fnm");
    
    FieldsWriter fieldsWriter =			  // merge field values
      new FieldsWriter(directory, segment, fieldInfos);
    try {
      for (int i = 0; i < readers.size(); i++) {
	SegmentReader reader = (SegmentReader)readers.elementAt(i);
	BitVector deletedDocs = reader.deletedDocs;
	int maxDoc = reader.maxDoc();
	for (int j = 0; j < maxDoc; j++)
	  if (deletedDocs == null || !deletedDocs.get(j)) // skip deleted docs
	    fieldsWriter.addDocument(reader.document(j));
      }
    } finally {
      fieldsWriter.close();
    }
  }

  private OutputStream freqOutput = null;
  private OutputStream proxOutput = null;
  private TermInfosWriter termInfosWriter = null;
  private SegmentMergeQueue queue = null;

  private final void mergeTerms() throws IOException {
    try {
      freqOutput = directory.createFile(segment + ".frq");
      proxOutput = directory.createFile(segment + ".prx");
      termInfosWriter =
	new TermInfosWriter(directory, segment, fieldInfos);
      
      mergeTermInfos();
      
    } finally {
      if (freqOutput != null) 		freqOutput.close();
      if (proxOutput != null) 		proxOutput.close();
      if (termInfosWriter != null) 	termInfosWriter.close();
      if (queue != null)		queue.close();
    }
  }

  private final void mergeTermInfos() throws IOException {
    queue = new SegmentMergeQueue(readers.size());
    int base = 0;
    for (int i = 0; i < readers.size(); i++) {
      SegmentReader reader = (SegmentReader)readers.elementAt(i);
      SegmentTermEnum termEnum = (SegmentTermEnum)reader.terms();
      SegmentMergeInfo smi = new SegmentMergeInfo(base, termEnum, reader);
      base += reader.numDocs();
      if (smi.next())
	queue.put(smi);				  // initialize queue
      else
	smi.close();
    }

    SegmentMergeInfo[] match = new SegmentMergeInfo[readers.size()];
    
    while (queue.size() > 0) {
      int matchSize = 0;			  // pop matching terms
      match[matchSize++] = (SegmentMergeInfo)queue.pop();
      Term term = match[0].term;
      SegmentMergeInfo top = (SegmentMergeInfo)queue.top();
      
      while (top != null && term.compareTo(top.term) == 0) {
	match[matchSize++] = (SegmentMergeInfo)queue.pop();
	top = (SegmentMergeInfo)queue.top();
      }

      mergeTermInfo(match, matchSize);		  // add new TermInfo
      
      while (matchSize > 0) {
	SegmentMergeInfo smi = match[--matchSize];
	if (smi.next())
	  queue.put(smi);			  // restore queue
	else
	  smi.close();				  // done with a segment
      }
    }
  }

  private final TermInfo termInfo = new TermInfo(); // minimize consing

  private final void mergeTermInfo(SegmentMergeInfo[] smis, int n)
       throws IOException {
    long freqPointer = freqOutput.getFilePointer();
    long proxPointer = proxOutput.getFilePointer();

    int df = appendPostings(smis, n);		  // append posting data

    if (df > 0) {
      // add an entry to the dictionary with pointers to prox and freq files
      termInfo.set(df, freqPointer, proxPointer);
      termInfosWriter.add(smis[0].term, termInfo);
    }
  }
       
  private final int appendPostings(SegmentMergeInfo[] smis, int n)
       throws IOException {
    int lastDoc = 0;
    int df = 0;					  // number of docs w/ term
    for (int i = 0; i < n; i++) {
      SegmentMergeInfo smi = smis[i];
      SegmentTermPositions postings = smi.postings;
      int base = smi.base;
      int[] docMap = smi.docMap;
      smi.termEnum.termInfo(termInfo);
      postings.seek(termInfo);
      while (postings.next()) {
	int doc;
	if (docMap == null)
	  doc = base + postings.doc;		  // no deletions
	else
	  doc = base + docMap[postings.doc];	  // re-map around deletions

	if (doc < lastDoc)
	  throw new IllegalStateException("docs out of order");

	int docCode = (doc - lastDoc) << 1;	  // use low bit to flag freq=1
	lastDoc = doc;

	int freq = postings.freq;
	if (freq == 1) {
	  freqOutput.writeVInt(docCode | 1);	  // write doc & freq=1
	} else {
	  freqOutput.writeVInt(docCode);	  // write doc
	  freqOutput.writeVInt(freq);		  // write frequency in doc
	}
	  
	int lastPosition = 0;			  // write position deltas
	for (int j = 0; j < freq; j++) {
	  int position = postings.nextPosition();
	  proxOutput.writeVInt(position - lastPosition);
	  lastPosition = position;
	}

	df++;
      }
    }
    return df;
  }

  private final void mergeNorms() throws IOException {
    for (int i = 0; i < fieldInfos.size(); i++) {
      FieldInfo fi = fieldInfos.fieldInfo(i);
      if (fi.isIndexed) {
	OutputStream output = directory.createFile(segment + ".f" + i);
	try {
	  for (int j = 0; j < readers.size(); j++) {
	    SegmentReader reader = (SegmentReader)readers.elementAt(j);
	    BitVector deletedDocs = reader.deletedDocs;
	    InputStream input = reader.normStream(fi.name);
            int maxDoc = reader.maxDoc();
	    try {
	      for (int k = 0; k < maxDoc; k++) {
		byte norm = input != null ? input.readByte() : (byte)0;
		if (deletedDocs == null || !deletedDocs.get(k))
		  output.writeByte(norm);
	      }
	    } finally {
	      if (input != null)
		input.close();
	    }
	  }
	} finally {
	  output.close();
	}
      }
    }
  }
}
