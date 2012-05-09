package org.apache.lucene.codecs.stacked;

import java.util.Map;

import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.SegmentReadState;

public class StackedMap {

  public static StackedMap read(SegmentReadState state) {
    StackedMap map = new StackedMap();
    return map;
  }
  
  /**
   * Return a map of field names and stacked document ids that
   * contain updated stored field values
   * @param oid original id
   * @return map where keys are field names and values are stacked document ids where
   * updated values can be found
   */
  public Map<String,Integer> getStackedStoredIds(int oid) {
    return null;
  }
  
  /**
   * Return a stacked document id that updates an inverted field for the
   * original id
   * @param field field name
   * @param oid original doc id
   * @return -1 if there is no such document, or the stacked document id.
   */
  public int getStackedDocsEnumId(String field, int oid) {
    // XXX
    return -1;
  }
  
  /**
   * Find the original id after the current id of a document with
   * updated postings.
   * @param field field name
   * @param currentId current original id
   * @return next original id of a document with updated postings
   */
  public int advanceDocsEnum(String field, int currentId) {
    // XXX
    return DocsEnum.NO_MORE_DOCS;
  }
}
