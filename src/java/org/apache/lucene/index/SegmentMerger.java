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
import java.util.ArrayList;
import java.util.Iterator;
import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.OutputStream;
import org.apache.lucene.store.InputStream;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.BitVector;

final class SegmentMerger {
  private boolean useCompoundFile;
  private Directory directory;
  private String segment;

  private Vector readers = new Vector();
  private FieldInfos fieldInfos;

  // File extensions of old-style index files
  private static final String COMPOUND_EXTENSIONS[] = new String[] {
    "fnm", "frq", "prx", "fdx", "fdt", "tii", "tis"
  };
  
  SegmentMerger(Directory dir, String name, boolean compoundFile) {
    directory = dir;
    segment = name;
    useCompoundFile = compoundFile;
  }

  final void add(IndexReader reader) {
    readers.addElement(reader);
  }

  final IndexReader segmentReader(int i) {
    return (IndexReader)readers.elementAt(i);
  }

  final int merge() throws IOException {
    int value;
    try {
      value = mergeFields();
      mergeTerms();
      mergeNorms();
    } finally {
      for (int i = 0; i < readers.size(); i++) {  // close readers
	IndexReader reader = (IndexReader)readers.elementAt(i);
	reader.close();
      }
    }
    
    if (useCompoundFile)
        createCompoundFile();

    return value;
  }

  private final void createCompoundFile() 
  throws IOException {
    CompoundFileWriter cfsWriter = 
        new CompoundFileWriter(directory, segment + ".cfs");
    
    ArrayList files = 
        new ArrayList(COMPOUND_EXTENSIONS.length + fieldInfos.size());    
    
    // Basic files
    for (int i=0; i<COMPOUND_EXTENSIONS.length; i++) {
        files.add(segment + "." + COMPOUND_EXTENSIONS[i]);
    }

    // Field norm files
    for (int i = 0; i < fieldInfos.size(); i++) {
      FieldInfo fi = fieldInfos.fieldInfo(i);
      if (fi.isIndexed) {
        files.add(segment + ".f" + i);
      }
    }

    // Now merge all added files
    Iterator it = files.iterator();
    while(it.hasNext()) {
      cfsWriter.addFile((String) it.next());
    }
    
    // Perform the merge
    cfsWriter.close();
        
    // Now delete the source files
    it = files.iterator();
    while(it.hasNext()) {
      directory.deleteFile((String) it.next());
    }
  }
  
  
  private final int mergeFields() throws IOException {
    fieldInfos = new FieldInfos();		  // merge field names
    int docCount = 0;
    for (int i = 0; i < readers.size(); i++) {
      IndexReader reader = (IndexReader)readers.elementAt(i);
      fieldInfos.add(reader.getFieldNames(true), true);
      fieldInfos.add(reader.getFieldNames(false), false);
    }
    fieldInfos.write(directory, segment + ".fnm");
    
    FieldsWriter fieldsWriter =			  // merge field values
      new FieldsWriter(directory, segment, fieldInfos);
    try {
      for (int i = 0; i < readers.size(); i++) {
	IndexReader reader = (IndexReader)readers.elementAt(i);
	int maxDoc = reader.maxDoc();
	for (int j = 0; j < maxDoc; j++)
	  if (!reader.isDeleted(j)){               // skip deleted docs
            fieldsWriter.addDocument(reader.document(j));
            docCount++;
	  }
      }
    } finally {
      fieldsWriter.close();
    }
    return docCount;
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
      IndexReader reader = (IndexReader)readers.elementAt(i);
      TermEnum termEnum = reader.terms();
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

    long skipPointer = writeSkip();

    if (df > 0) {
      // add an entry to the dictionary with pointers to prox and freq files
      termInfo.set(df, freqPointer, proxPointer, (int)(skipPointer-freqPointer));
      termInfosWriter.add(smis[0].term, termInfo);
    }
  }

  private final int appendPostings(SegmentMergeInfo[] smis, int n)
       throws IOException {
    final int skipInterval = termInfosWriter.skipInterval;
    int lastDoc = 0;
    int df = 0;					  // number of docs w/ term
    resetSkip();
    for (int i = 0; i < n; i++) {
      SegmentMergeInfo smi = smis[i];
      TermPositions postings = smi.postings;
      int base = smi.base;
      int[] docMap = smi.docMap;
      postings.seek(smi.termEnum);
      while (postings.next()) {
        int doc = postings.doc();
        if (docMap != null)
          doc = docMap[doc];                      // map around deletions
        doc += base;                              // convert to merged space

        if (doc < lastDoc)
          throw new IllegalStateException("docs out of order");

        df++;

        if ((df % skipInterval) == 0) {
          bufferSkip(lastDoc);
        }

        int docCode = (doc - lastDoc) << 1;	  // use low bit to flag freq=1
        lastDoc = doc;
        
        int freq = postings.freq();
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
      }
    }
    return df;
  }

  private RAMOutputStream skipBuffer = new RAMOutputStream();
  private int lastSkipDoc;
  private long lastSkipFreqPointer;
  private long lastSkipProxPointer;

  private void resetSkip() throws IOException {
    skipBuffer.reset();
    lastSkipDoc = 0;
    lastSkipFreqPointer = freqOutput.getFilePointer();
    lastSkipProxPointer = proxOutput.getFilePointer();
  }

  private void bufferSkip(int doc) throws IOException {
    long freqPointer = freqOutput.getFilePointer();
    long proxPointer = proxOutput.getFilePointer();

    skipBuffer.writeVInt(doc - lastSkipDoc); 
    skipBuffer.writeVInt((int)(freqPointer - lastSkipFreqPointer));
    skipBuffer.writeVInt((int)(proxPointer - lastSkipProxPointer));

    lastSkipDoc = doc;
    lastSkipFreqPointer = freqPointer;
    lastSkipProxPointer = proxPointer;
  }

  private long writeSkip() throws IOException {
    long skipPointer = freqOutput.getFilePointer();
    skipBuffer.writeTo(freqOutput);
    return skipPointer;
  }

  private void mergeNorms() throws IOException {
    for (int i = 0; i < fieldInfos.size(); i++) {
      FieldInfo fi = fieldInfos.fieldInfo(i);
      if (fi.isIndexed) {
	OutputStream output = directory.createFile(segment + ".f" + i);
	try {
	  for (int j = 0; j < readers.size(); j++) {
	    IndexReader reader = (IndexReader)readers.elementAt(j);
	    byte[] input = reader.norms(fi.name);
            int maxDoc = reader.maxDoc();
            for (int k = 0; k < maxDoc; k++) {
              byte norm = input != null ? input[k] : (byte)0;
              if (!reader.isDeleted(k)) {
                output.writeByte(norm);
              }
	    }
	  }
	} finally {
	  output.close();
	}
      }
    }
  }

}
