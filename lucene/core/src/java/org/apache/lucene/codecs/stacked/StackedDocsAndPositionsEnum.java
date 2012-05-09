package org.apache.lucene.codecs.stacked;

import java.io.IOException;

import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

public class StackedDocsAndPositionsEnum extends DocsAndPositionsEnum {
  private String field;
  private StackedMap map;
  private BytesRef term;
  private DocsAndPositionsEnum main;
  private DocsAndPositionsEnum stacked = null, currentStacked = null;
  private TermsEnum stackedTerms;
  private boolean needsOffsets;
  private Bits liveDocs;
  private int currentMID = -1;
  private int currentSID = -1;
  private int currentFreq;
  
  public StackedDocsAndPositionsEnum(String field, StackedMap map, BytesRef term,
      DocsAndPositionsEnum main,
      TermsEnum stackedTerms, boolean needsOffsets, Bits liveDocs) {
    this.field = field;
    this.map = map;
    this.term = term;
    this.main = main;
    this.stackedTerms = stackedTerms;
    this.needsOffsets = needsOffsets;
    this.liveDocs = liveDocs;
  }

  @Override
  public int nextPosition() throws IOException {
    if (currentStacked != null) {
      return currentStacked.nextPosition();
    } else {
      return main.nextPosition();
    }
  }
  
  @Override
  public int startOffset() throws IOException {
    if (currentStacked != null) {
      return currentStacked.startOffset();
    } else {
      return main.startOffset();
    }
  }
  
  @Override
  public int endOffset() throws IOException {
    if (currentStacked != null) {
      return currentStacked.endOffset();
    } else {
      return main.endOffset();
    }
  }
  
  @Override
  public BytesRef getPayload() throws IOException {
    if (currentStacked != null) {
      return currentStacked.getPayload();
    } else {
      return main.getPayload();
    }
  }
  
  @Override
  public boolean hasPayload() {
    if (currentStacked != null) {
      return currentStacked.hasPayload();
    } else {
      return main.hasPayload();
    }
  }
  
  @Override
  public int freq() {
    return currentFreq;
  }
  
  @Override
  public int docID() {
    return Math.min(currentMID, currentSID);
  }
  
  @Override
  public int nextDoc() throws IOException {
    if (currentMID == -1 || currentMID < currentSID) { // init
      currentMID = main.nextDoc();
    }
    if (currentSID == -1 || currentSID < currentMID) { // init
      currentSID = map.advanceDocsEnum(field, currentSID);
    }
    return Math.min(currentMID, currentSID);
  }
  
  @Override
  public int advance(int target) throws IOException {
    if (currentMID == -1 || currentMID < target) {
      currentMID = main.advance(target);
    }
    if (currentSID == -1 || currentSID < target) {
      currentSID = map.advanceDocsEnum(field, target);
    }
    // prepare freq
    if (currentSID <= currentMID) {
      // !!!!!!!!! MAJOR COST !!!!!!!!
      if (stackedTerms.seekExact(term, true)) { // found this term
        stacked = stackedTerms.docsAndPositions(liveDocs, stacked, needsOffsets);
        // remap;
        int newID = map.getStackedDocsEnumId(field, currentSID);
        stacked.advance(newID);
        currentFreq = stacked.freq();
        currentStacked = stacked;
      } else { // term not found
        currentSID = currentMID;
        currentFreq = main.freq();
        currentStacked = null;
      }
    } else {
      currentFreq = main.freq();
      currentStacked = null;
    }
    return Math.min(currentMID, currentSID);
  }
  
}
