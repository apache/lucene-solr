package org.apache.lucene.index;
import java.util.*;

/**
 */
class SegmentTermVector implements TermFreqVector {
  private String field;
  private String terms[];
  private int termFreqs[];
  
  SegmentTermVector(String field, String terms[], int termFreqs[]) {
    this.field = field;
    this.terms = terms;
    this.termFreqs = termFreqs;
  }

  /**
   * 
   * @return The number of the field this vector is associated with
   */
  public String getField() {
    return field;
  }

  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append('{');
    sb.append(field).append(": ");
    for (int i=0; i<terms.length; i++) {
      if (i>0) sb.append(", ");
      sb.append(terms[i]).append('/').append(termFreqs[i]);
    }
    sb.append('}');
    return sb.toString();
  }

  public int size() {
    return terms == null ? 0 : terms.length;
  }

  public String [] getTerms() {
    return terms;
  }

  public int[] getTermFrequencies() {
    return termFreqs;
  }

  public int indexOf(String termText) {
    int res = Arrays.binarySearch(terms, termText);
    return res >= 0 ? res : -1;
  }

  public int[] indexesOf(String [] termNumbers, int start, int len) {
    // TODO: there must be a more efficient way of doing this.
    //       At least, we could advance the lower bound of the terms array
    //       as we find valid indexes. Also, it might be possible to leverage
    //       this even more by starting in the middle of the termNumbers array
    //       and thus dividing the terms array maybe in half with each found index.
    int res[] = new int[len];

    for (int i=0; i < len; i++) {
      res[i] = indexOf(termNumbers[i]);
    }
    return res;
  }
}
