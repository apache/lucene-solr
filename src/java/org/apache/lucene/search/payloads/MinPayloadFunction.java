package org.apache.lucene.search.payloads;

import org.apache.lucene.index.Term;


/**
 * Calculates the miniumum payload seen
 *
 **/
public class MinPayloadFunction extends PayloadFunction {

    public float currentScore(int docId, String field, int start, int end, int numPayloadsSeen, float currentScore, float currentPayloadScore) {
    return Math.min(currentPayloadScore, currentScore);
  }

  public float docScore(int docId, String field, int numPayloadsSeen, float payloadScore) {
    return numPayloadsSeen > 0 ? payloadScore : 1;
  }

}
