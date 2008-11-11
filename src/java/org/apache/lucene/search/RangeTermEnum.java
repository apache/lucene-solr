package org.apache.lucene.search;

import java.io.IOException;
import java.text.Collator;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;

/**
 * Subclass of FilteredTermEnum for enumerating all terms that match the
 * specified range parameters.
 * <p>
 * Term enumerations are always ordered by Term.compareTo().  Each term in
 * the enumeration is greater than all that precede it.
 */
public class RangeTermEnum extends FilteredTermEnum {

  private Collator collator = null;
  private boolean endEnum = false;
  private String field;
  private String upperTermText;
  private String lowerTermText;
  private boolean includeLower;
  private boolean includeUpper;

  /**
   * Enumerates all terms greater/equal than <code>lowerTerm</code>
   * but less/equal than <code>upperTerm</code>. 
   * 
   * If an endpoint is null, it is said to be "open". Either or both 
   * endpoints may be open.  Open endpoints may not be exclusive 
   * (you can't select all but the first or last term without 
   * explicitly specifying the term to exclude.)
   * 
   * @param reader
   * @param collator
   *          The collator to use to collate index Terms, to determine their
   *          membership in the range bounded by <code>lowerTerm</code> and
   *          <code>upperTerm</code>.
   * @param field
   *          An interned field that holds both lower and upper terms.
   * @param lowerTermText
   *          The term text at the lower end of the range
   * @param upperTermText
   *          The term text at the upper end of the range
   * @param includeLower
   *          If true, the <code>lowerTerm</code> is included in the range.
   * @param includeUpper
   *          If true, the <code>upperTerm</code> is included in the range.
   * 
   * @throws IOException
   */
  public RangeTermEnum(IndexReader reader, Collator collator, String field,
      String lowerTermText, String upperTermText, boolean includeLower, boolean includeUpper) throws IOException {
    this.collator = collator;
    this.upperTermText = upperTermText;
    this.lowerTermText = lowerTermText;
    this.includeLower = includeLower;
    this.includeUpper = includeUpper;
    this.field = field;
    
    // do a little bit of normalization...
    // open ended range queries should always be inclusive.
    if (this.lowerTermText == null) {
      this.lowerTermText = "";
      this.includeLower = true;
    }
    
    if (this.upperTermText == null) {
      this.includeUpper = true;
    }

    setEnum(reader.terms(new Term(this.field, this.lowerTermText)));
  }

  public float difference() {
    return 1.0f;
  }

  protected boolean endEnum() {
    return endEnum;
  }

  protected boolean termCompare(Term term) {
    if (collator == null) {
      // Use Unicode code point ordering
      boolean checkLower = false;
      if (!includeLower) // make adjustments to set to exclusive
        checkLower = true;
      if (term != null && term.field() == field) { // interned comparison
        if (!checkLower || null==lowerTermText || term.text().compareTo(lowerTermText) > 0) {
          checkLower = false;
          if (upperTermText != null) {
            int compare = upperTermText.compareTo(term.text());
            /*
             * if beyond the upper term, or is exclusive and this is equal to
             * the upper term, break out
             */
            if ((compare < 0) ||
                (!includeUpper && compare==0)) {
              endEnum = true;
              return false;
            }
          }
          return true;
        }
      } else {
        // break
        endEnum = true;
        return false;
      }
      return false;
    } else {
      if (term != null && term.field() == field) { // interned comparison
        if ((lowerTermText == null
            || (includeLower
                ? collator.compare(term.text(), lowerTermText) >= 0
                : collator.compare(term.text(), lowerTermText) > 0))
           && (upperTermText == null
               || (includeUpper
                   ? collator.compare(term.text(), upperTermText) <= 0
                   : collator.compare(term.text(), upperTermText) < 0))) {
          return true;
        }
        return false;
      }
      endEnum = true;
      return false;
    }
  }
}
