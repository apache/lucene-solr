package org.apache.lucene.beans;

import org.apache.lucene.beans.IndividualHit;


public class CompareDocumentsByField implements java.util.Comparator {
  public int compare(Object hit1, Object hit2) {
    String myDate1 = ((IndividualHit) hit1).getField();
    String myDate2 = ((IndividualHit) hit2).getField();
    if ((myDate1 == null) || (myDate2 == null)) {
//logger.error("A date was null, the score is "+((IndividualHit) hit1).getScore());
//return -1;
    }
    return -1 * (myDate1.compareTo(myDate2)); 	//sort in descending order
  }

  public boolean equals(Object o1) {
    return false;
  }
}