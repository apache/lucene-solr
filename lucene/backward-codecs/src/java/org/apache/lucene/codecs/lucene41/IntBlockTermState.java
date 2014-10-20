package org.apache.lucene.codecs.lucene41;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.index.TermState;

/**
 * term state for Lucene 4.1 postings format
 * @deprecated only for reading old 4.x segments
 */
@Deprecated
final class IntBlockTermState extends BlockTermState {
  long docStartFP = 0;
  long posStartFP = 0;
  long payStartFP = 0;
  long skipOffset = -1;
  long lastPosBlockOffset = -1;
  // docid when there is a single pulsed posting, otherwise -1
  // freq is always implicitly totalTermFreq in this case.
  int singletonDocID = -1;

  @Override
  public IntBlockTermState clone() {
    IntBlockTermState other = new IntBlockTermState();
    other.copyFrom(this);
    return other;
  }

  @Override
  public void copyFrom(TermState _other) {
    super.copyFrom(_other);
    IntBlockTermState other = (IntBlockTermState) _other;
    docStartFP = other.docStartFP;
    posStartFP = other.posStartFP;
    payStartFP = other.payStartFP;
    lastPosBlockOffset = other.lastPosBlockOffset;
    skipOffset = other.skipOffset;
    singletonDocID = other.singletonDocID;
  }


  @Override
  public String toString() {
    return super.toString() + " docStartFP=" + docStartFP + " posStartFP=" + posStartFP + " payStartFP=" + payStartFP + " lastPosBlockOffset=" + lastPosBlockOffset + " singletonDocID=" + singletonDocID;
  }
}