package org.apache.lucene.index;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;

final class DocumentWriter {
  private Analyzer analyzer;
  private Directory directory;
  private Similarity similarity;
  private FieldInfos fieldInfos;
  private int maxFieldLength;
  private int termIndexInterval = IndexWriter.DEFAULT_TERM_INDEX_INTERVAL;
  private PrintStream infoStream;

  /** This ctor used by test code only.
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

  DocumentWriter(Directory directory, Analyzer analyzer, IndexWriter writer) {
    this.directory = directory;
    this.analyzer = analyzer;
    this.similarity = writer.getSimilarity();
    this.maxFieldLength = writer.getMaxFieldLength();
    this.termIndexInterval = writer.getTermIndexInterval();
  }

  final void addDocument(String segment, Document doc)
          throws CorruptIndexException, IOException {
    // create field infos
    fieldInfos = new FieldInfos();
    fieldInfos.add(doc);
    
    // invert doc into postingTable
    postingTable.clear();			  // clear postingTable
    fieldLengths = new int[fieldInfos.size()];    // init fieldLengths
    fieldPositions = new int[fieldInfos.size()];  // init fieldPositions
    fieldOffsets = new int[fieldInfos.size()];    // init fieldOffsets
    fieldStoresPayloads = new BitSet(fieldInfos.size());
    
    fieldBoosts = new float[fieldInfos.size()];	  // init fieldBoosts
    Arrays.fill(fieldBoosts, doc.getBoost());

    // Before we write the FieldInfos we invert the Document. The reason is that
    // during invertion the TokenStreams of tokenized fields are being processed 
    // and we might encounter tokens that have payloads associated with them. In 
    // this case we have to update the FieldInfo of the particular field.
    invertDocument(doc);

    // sort postingTable into an array
    Posting[] postings = sortPostingTable();
    
    // write field infos 
    fieldInfos.write(directory, segment + ".fnm");

    // write field values
    FieldsWriter fieldsWriter =
            new FieldsWriter(directory, segment, fieldInfos);
    try {
      fieldsWriter.addDocument(doc);
    } finally {
      fieldsWriter.close();
    }
    
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
    writeNorms(segment);

  }

  // Keys are Terms, values are Postings.
  // Used to buffer a document before it is written to the index.
  private final Hashtable postingTable = new Hashtable();
  private int[] fieldLengths;
  private int[] fieldPositions;
  private int[] fieldOffsets;
  private float[] fieldBoosts;
  
  // If any of the tokens of a paticular field carry a payload
  // then we enable payloads for that field. 
  private BitSet fieldStoresPayloads;

  // Tokenizes the fields of a document into Postings.
  private final void invertDocument(Document doc)
          throws IOException {
    Iterator fieldIterator = doc.getFields().iterator();
    while (fieldIterator.hasNext()) {
      Fieldable field = (Fieldable) fieldIterator.next();
      String fieldName = field.name();
      int fieldNumber = fieldInfos.fieldNumber(fieldName);

      int length = fieldLengths[fieldNumber];     // length of field
      int position = fieldPositions[fieldNumber]; // position in field
      if (length>0) position+=analyzer.getPositionIncrementGap(fieldName);
      int offset = fieldOffsets[fieldNumber];       // offset field

      if (field.isIndexed()) {
        if (!field.isTokenized()) {		  // un-tokenized field
          String stringValue = field.stringValue();
          if(field.isStoreOffsetWithTermVector())
            addPosition(fieldName, stringValue, position++, null, new TermVectorOffsetInfo(offset, offset + stringValue.length()));
          else
            addPosition(fieldName, stringValue, position++, null, null);
          offset += stringValue.length();
          length++;
        } else 
        { // tokenized field
          TokenStream stream = field.tokenStreamValue();
          
          // the field does not have a TokenStream,
          // so we have to obtain one from the analyzer
          if (stream == null) {
            Reader reader;			  // find or make Reader
            if (field.readerValue() != null)
              reader = field.readerValue();
            else if (field.stringValue() != null)
              reader = new StringReader(field.stringValue());
            else
              throw new IllegalArgumentException
                      ("field must have either String or Reader value");
  
            // Tokenize field and add to postingTable
            stream = analyzer.tokenStream(fieldName, reader);
          }
          
          // reset the TokenStream to the first token
          stream.reset();
          
          try {
            Token lastToken = null;
            for (Token t = stream.next(); t != null; t = stream.next()) {
              position += (t.getPositionIncrement() - 1);
              
              Payload payload = t.getPayload();
              if (payload != null) {
                // enable payloads for this field
              	fieldStoresPayloads.set(fieldNumber);
              }
              
              TermVectorOffsetInfo termVectorOffsetInfo;
              if (field.isStoreOffsetWithTermVector()) {
                termVectorOffsetInfo = new TermVectorOffsetInfo(offset + t.startOffset(), offset + t.endOffset());
              } else {
                termVectorOffsetInfo = null;
              }
              addPosition(fieldName, t.termText(), position++, payload, termVectorOffsetInfo);
              
              lastToken = t;
              if (++length >= maxFieldLength) {
                if (infoStream != null)
                  infoStream.println("maxFieldLength " +maxFieldLength+ " reached, ignoring following tokens");
                break;
              }
            }
            
            if(lastToken != null)
              offset += lastToken.endOffset() + 1;
            
          } finally {
            stream.close();
          }
        }

        fieldLengths[fieldNumber] = length;	  // save field length
        fieldPositions[fieldNumber] = position;	  // save field position
        fieldBoosts[fieldNumber] *= field.getBoost();
        fieldOffsets[fieldNumber] = offset;
      }
    }
    
    // update fieldInfos for all fields that have one or more tokens with payloads
    for (int i = fieldStoresPayloads.nextSetBit(0); i >= 0; i = fieldStoresPayloads.nextSetBit(i+1)) { 
    	fieldInfos.fieldInfo(i).storePayloads = true;
    }
  }

  private final Term termBuffer = new Term("", ""); // avoid consing

  private final void addPosition(String field, String text, int position, Payload payload, TermVectorOffsetInfo offset) {
    termBuffer.set(field, text);
    //System.out.println("Offset: " + offset);
    Posting ti = (Posting) postingTable.get(termBuffer);
    if (ti != null) {				  // word seen before
      int freq = ti.freq;
      if (ti.positions.length == freq) {	  // positions array is full
        int[] newPositions = new int[freq * 2];	  // double size
        int[] positions = ti.positions;
        System.arraycopy(positions, 0, newPositions, 0, freq);
        ti.positions = newPositions;
        
        if (ti.payloads != null) {
          // the current field stores payloads
          Payload[] newPayloads = new Payload[freq * 2];  // grow payloads array
          Payload[] payloads = ti.payloads;
          System.arraycopy(payloads, 0, newPayloads, 0, payloads.length);
          ti.payloads = newPayloads;
        }
      }
      ti.positions[freq] = position;		  // add new position

      if (payload != null) {
        if (ti.payloads == null) {
          // lazily allocate payload array
          ti.payloads = new Payload[ti.positions.length];
        }
        ti.payloads[freq] = payload;
      }
      
      if (offset != null) {
        if (ti.offsets.length == freq){
          TermVectorOffsetInfo [] newOffsets = new TermVectorOffsetInfo[freq*2];
          TermVectorOffsetInfo [] offsets = ti.offsets;
          System.arraycopy(offsets, 0, newOffsets, 0, freq);
          ti.offsets = newOffsets;
        }
        ti.offsets[freq] = offset;
      }
      ti.freq = freq + 1;			  // update frequency
    } else {					  // word not seen before
      Term term = new Term(field, text, false);
      postingTable.put(term, new Posting(term, position, payload, offset));
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
          throws CorruptIndexException, IOException {
    IndexOutput freq = null, prox = null;
    TermInfosWriter tis = null;
    TermVectorsWriter termVectorWriter = null;
    try {
      //open files for inverse index storage
      freq = directory.createOutput(segment + ".frq");
      prox = directory.createOutput(segment + ".prx");
      tis = new TermInfosWriter(directory, segment, fieldInfos,
                                termIndexInterval);
      TermInfo ti = new TermInfo();
      String currentField = null;
      boolean currentFieldHasPayloads = false;
      
      for (int i = 0; i < postings.length; i++) {
        Posting posting = postings[i];

        // check to see if we switched to a new field
        String termField = posting.term.field();
        if (currentField != termField) {
          // changing field - see if there is something to save
          currentField = termField;
          FieldInfo fi = fieldInfos.fieldInfo(currentField);
          currentFieldHasPayloads = fi.storePayloads;
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
        Payload[] payloads = posting.payloads;
        int lastPayloadLength = -1;
        
        
        // The following encoding is being used for positions and payloads:
        // Case 1: current field does not store payloads
        //           Positions     -> <PositionDelta>^freq
        //           PositionDelta -> VInt
        //         The PositionDelta is the difference between the current
        //         and the previous position
        // Case 2: current field stores payloads
        //           Positions     -> <PositionDelta, Payload>^freq
        //           Payload       ->  <PayloadLength?, PayloadData>
        //           PositionDelta -> VInt
        //           PayloadLength -> VInt
        //           PayloadData   -> byte^PayloadLength
        //         In this case PositionDelta/2 is the difference between
        //         the current and the previous position. If PositionDelta
        //         is odd, then a PayloadLength encoded as VInt follows,
        //         if PositionDelta is even, then it is assumed that the
        //         length of the current Payload equals the length of the
        //         previous Payload.        
        for (int j = 0; j < postingFreq; j++) {		  // use delta-encoding
          int position = positions[j];
          int delta = position - lastPosition;
          if (currentFieldHasPayloads) {
            int payloadLength = 0;
            Payload payload = null;
            if (payloads != null) {
              payload = payloads[j];
              if (payload != null) {
                payloadLength = payload.length;
              }
            }
            if (payloadLength == lastPayloadLength) {
            	// the length of the current payload equals the length
            	// of the previous one. So we do not have to store the length
            	// again and we only shift the position delta by one bit
              prox.writeVInt(delta * 2);
            } else {
            	// the length of the current payload is different from the
            	// previous one. We shift the position delta, set the lowest
            	// bit and store the current payload length as VInt.
              prox.writeVInt(delta * 2 + 1);
              prox.writeVInt(payloadLength);
              lastPayloadLength = payloadLength;
            }
            if (payloadLength > 0) {
            	// write current payload
              prox.writeBytes(payload.data, payload.offset, payload.length);
            }
          } else {
          	// field does not store payloads, just write position delta as VInt
            prox.writeVInt(delta);
          }
          lastPosition = position;
        }
        if (termVectorWriter != null && termVectorWriter.isFieldOpen()) {
            termVectorWriter.addTerm(posting.term.text(), postingFreq, posting.positions, posting.offsets);
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

  private final void writeNorms(String segment) throws IOException { 
    for(int n = 0; n < fieldInfos.size(); n++){
      FieldInfo fi = fieldInfos.fieldInfo(n);
      if(fi.isIndexed && !fi.omitNorms){
        float norm = fieldBoosts[n] * similarity.lengthNorm(fi.name, fieldLengths[n]);
        IndexOutput norms = directory.createOutput(segment + ".f" + n);
        try {
          norms.writeByte(Similarity.encodeNorm(norm));
        } finally {
          norms.close();
        }
      }
    }
  }
  
  /** If non-null, a message will be printed to this if maxFieldLength is reached.
   */
  void setInfoStream(PrintStream infoStream) {
    this.infoStream = infoStream;
  }

  int getNumFields() {
    return fieldInfos.size();
  }
}

final class Posting {				  // info about a Term in a doc
  Term term;					  // the Term
  int freq;					  // its frequency in doc
  int[] positions;				  // positions it occurs at
  Payload[] payloads; // the payloads of the terms
  TermVectorOffsetInfo [] offsets;
  

  Posting(Term t, int position, Payload payload, TermVectorOffsetInfo offset) {
    term = t;
    freq = 1;
    positions = new int[1];
    positions[0] = position;
    
    if (payload != null) {
      payloads = new Payload[1];
      payloads[0] = payload;
    } else 
      payloads = null;    
    

    if(offset != null){
      offsets = new TermVectorOffsetInfo[1];
      offsets[0] = offset;
    } else
      offsets = null;
  }
}
