package org.apache.solr.common.luke;

/**
 * @version $Id: AdminHandlers.java 608150 2008-01-02 17:15:30Z ryan $
 * @since solr 1.3
 */
public enum FieldFlag {
  INDEXED('I', "Indexed"), 
  TOKENIZED('T', "Tokenized"), 
  STORED('S', "Stored"), 
  MULTI_VALUED('M', "Multivalued"),
  TERM_VECTOR_STORED('V', "TermVector Stored"), 
  TERM_VECTOR_OFFSET('o', "Store Offset With TermVector"),
  TERM_VECTOR_POSITION('p', "Store Position With TermVector"),
  OMIT_NORMS('O', "Omit Norms"), 
  LAZY('L', "Lazy"), 
  BINARY('B', "Binary"), 
  COMPRESSED('C', "Compressed"),
  SORT_MISSING_FIRST('f', "Sort Missing First"), 
  SORT_MISSING_LAST('l', "Sort Missing Last");

  private final char abbreviation;
  private final String display;

  FieldFlag(char abbreviation, String display) {
    this.abbreviation = abbreviation;
    this.display = display;
    this.display.intern();//QUESTION:  Need we bother here?
  }

  public static FieldFlag getFlag(char abbrev){
    FieldFlag result = null;
    FieldFlag [] vals = FieldFlag.values();
    for (int i = 0; i < vals.length; i++) {
      if (vals[i].getAbbreviation() == abbrev){
        result = vals[i];
        break;
      }
    }
    return result;
  }

  public char getAbbreviation() {
    return abbreviation;
  }

  public String getDisplay() {
    return display;
  }
}
