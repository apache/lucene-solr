package org.apache.lucene.codecs.stacked;

import java.io.IOException;

import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

public class StackedDocsEnum extends DocsEnum {
  private String field;
  private StackedMap map;
  private BytesRef term;
  private DocsEnum main;
  private TermsEnum stackedTerms;
  private boolean needsFreqs;
  private int currentMID = -1;
  private int currentSID = -1;
  private int currentFreq;
  private DocsEnum stacked;
  
  public StackedDocsEnum(String field, StackedMap map, BytesRef term, DocsEnum main, TermsEnum stackedTerms,
      boolean needsFreqs, Bits liveDocs) {
    this.field = field;
    this.map = map;
    this.term = term;
    this.main = main;
    this.stackedTerms = stackedTerms;
    this.needsFreqs = needsFreqs;
    stacked = null;
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
    if (needsFreqs) { // prepare freq
      if (currentSID <= currentMID) {
        // !!!!!!!!! MAJOR COST !!!!!!!!
        if (stackedTerms.seekExact(term, true)) { // found this term in updates
          stacked = stackedTerms.docs(null, stacked, needsFreqs);
          // remap;
          int newID = map.getStackedDocsEnumId(field, currentSID);
          stacked.advance(newID);
          currentFreq = stacked.freq();
        } else { // term not found - use main
          currentSID = currentMID;
          currentFreq = main.freq();
        }
      } else {
        currentFreq = main.freq();
      }
    }
    return Math.min(currentMID, currentSID);
  }
  
}
