package org.apache.lucene.store.instantiated;

/**
 * Copyright 2006 The Apache Software Foundation
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

import org.apache.lucene.index.BulkPostingsEnum;

public class InstantiatedBulkPostingsEnum extends BulkPostingsEnum {

  private final DocDeltasReader docDeltasReader;
  private final FreqsReader freqsReader;
  private final PositionDeltasReader positionDeltasReader;
  private final String field;

  private InstantiatedTerm term;

  public InstantiatedBulkPostingsEnum(String field, boolean doFreq, boolean doPositions) {
    this.field = field;
    docDeltasReader = new DocDeltasReader();
    if (doFreq) {
      freqsReader = new FreqsReader();
    } else {
      freqsReader = null;
    }

    if (doPositions) {
      positionDeltasReader = new PositionDeltasReader();
    } else {
      positionDeltasReader = null;
    }
  }

  public boolean canReuse(String field, boolean doFreq, boolean doPositions) {
    return field.equals(this.field) && (doFreq == (freqsReader != null)) && (doPositions == (positionDeltasReader != null));
  }

  private class DocDeltasReader extends BlockReader {
    private final int[] buffer = new int[64];
    private InstantiatedTermDocumentInformation[] docs;
    private int docUpto;
    private int lastDocID;
    private int limit;

    public void reset(InstantiatedTerm term) {
      docUpto = 0;
      lastDocID = 0;
      docs = term.getAssociatedDocuments();
      fill();
    }

    public void jump(int docUpto, int lastDocID) {
      this.lastDocID = lastDocID;
      this.docUpto = docUpto;
      this.limit = 0;
    }

    @Override
    public int[] getBuffer() {
      return buffer;
    }

    @Override
    public int offset() {
      return 0;
    }

    @Override
    public void setOffset(int v) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int end() {
      return limit;
    }

    @Override
    public int fill() {
      final int chunk = Math.min(buffer.length, docs.length-docUpto);
      for(int i=0;i<chunk;i++) {
        final int docID = docs[docUpto++].getDocument().getDocumentNumber();
        buffer[i] = docID - lastDocID;
        lastDocID = docID;
      }
      docUpto += chunk;
      return limit = chunk;
    }
  }

  private class FreqsReader extends BlockReader {
    private final int[] buffer = new int[64];
    private InstantiatedTermDocumentInformation[] docs;
    private int docUpto;
    private int limit;

    public void reset(InstantiatedTerm term) {
      docUpto = 0;
      docs = term.getAssociatedDocuments();
      fill();
    }

    public void jump(int docUpto, int lastDocID) {
      this.docUpto = docUpto;
      this.limit = 0;
    }

    @Override
    public int[] getBuffer() {
      return buffer;
    }

    @Override
    public int offset() {
      return 0;
    }

    @Override
    public void setOffset(int v) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int end() {
      return limit;
    }

    @Override
    public int fill() {
      final int chunk = Math.min(buffer.length, docs.length-docUpto);
      for(int i=0;i<chunk;i++) {
        buffer[i] =  docs[docUpto++].getTermPositions().length;
      }
      docUpto += chunk;
      return limit = chunk;
    }
  }

  private class PositionDeltasReader extends BlockReader {
    private final int[] buffer = new int[64];
    private InstantiatedTermDocumentInformation[] docs;
    private int docUpto;
    private int posUpto;
    private int limit;

    public void reset(InstantiatedTerm term) {
      docUpto = posUpto = 0;
      docs = term.getAssociatedDocuments();
      fill();
    }

    public void jump(int docUpto, int lastDocID) {
      this.docUpto = docUpto;
      posUpto = 0;
      this.limit = 0;
    }

    @Override
    public int[] getBuffer() {
      return buffer;
    }

    @Override
    public int offset() {
      return 0;
    }

    @Override
    public void setOffset(int v) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int end() {
      return limit;
    }

    @Override
    public int fill() {
      int upto = 0;
      while(docUpto < docs.length) {
        final InstantiatedTermDocumentInformation doc = docs[docUpto];
        final int[] positions = doc.getTermPositions();
        final int chunk = Math.min(buffer.length - upto, positions.length - posUpto);
        System.arraycopy(positions, posUpto, buffer, upto, chunk);
        
        upto += chunk;
        posUpto += chunk;
        if (posUpto == positions.length) {
          docUpto++;
          posUpto = 0;
        }
        if (upto == buffer.length) {
          break;
        }
      }
      return limit = upto;
    }
  }

  public InstantiatedBulkPostingsEnum reset(InstantiatedTerm term) {
    this.term = term;

    docDeltasReader.reset(term);
    
    if (freqsReader != null) {
      freqsReader.reset(term);
    }
    if (positionDeltasReader != null) {
      positionDeltasReader.reset(term);
    }
    return this;
  }

  @Override
  public BlockReader getDocDeltasReader() {
    return docDeltasReader;
  }

  @Override
  public BlockReader getPositionDeltasReader() {
    return positionDeltasReader;
  }

  @Override
  public BlockReader getFreqsReader() {
    return freqsReader;
  }

  private final JumpResult jumpResult = new JumpResult();

  @Override
  public JumpResult jump(int target, int curCount) {
    int docUpto = term.seekCeilingDocumentInformationIndex(target, 0);
    if (docUpto == -1) {
      // TODO: the bulk API currently can't express this
      // ("jumped beyond last doc")... because the skip data
      // for the core codecs doesn't "know" the last doc
      return null;
    }

    final int lastDocID = docUpto == 0 ? 0 : term.getAssociatedDocuments()[docUpto-1].getDocument().getDocumentNumber();
    docDeltasReader.jump(lastDocID, docUpto);
    if (freqsReader != null) {
      freqsReader.jump(lastDocID, docUpto);
    }
    if (positionDeltasReader != null) {
      positionDeltasReader.jump(lastDocID, docUpto);
    }

    jumpResult.docID = term.getAssociatedDocuments()[docUpto].getDocument().getDocumentNumber();
    jumpResult.count = docUpto;

    return jumpResult;
  }
}
