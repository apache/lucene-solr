package org.apache.lucene.search.spans;

import java.io.IOException;

/** Expert: an enumeration of span matches.  Used to implement span searching.
 * Each span represents a range of term positions within a document.  Matches
 * are enumerated in order, by increasing document number, within that by
 * increasing start position and finally by increasing end position. */
public interface Spans {
  /** Move to the next match, returning true iff any such exists. */
  boolean next() throws IOException;

  /** Skips to the first match beyond the current whose document number is
   * greater than or equal to <i>target</i>. <p>Returns true iff there is such
   * a match.  <p>Behaves as if written: <pre>
   *   boolean skipTo(int target) {
   *     do {
   *       if (!next())
   * 	     return false;
   *     } while (target > doc());
   *     return true;
   *   }
   * </pre>
   * Most implementations are considerably more efficient than that.
   */
  boolean skipTo(int target) throws IOException;

  /** Returns the document number of the current match.  Initially invalid. */
  int doc();

  /** Returns the start position of the current match.  Initially invalid. */
  int start();

  /** Returns the end position of the current match.  Initially invalid. */
  int end();

}
