package org.apache.solr.util;

public interface MoreLikeThisParams 
{
  // enable more like this -- this only applies to 'StandardRequestHandler' maybe DismaxRequestHandler
  public final static String MLT = "mlt";
  
  public final static String PREFIX = "mlt.";
  
  public final static String SIMILARITY_FIELDS     = PREFIX + "fl";
  public final static String MIN_TERM_FREQ         = PREFIX + "mintf";
  public final static String MIN_DOC_FREQ          = PREFIX + "mindf";
  public final static String MIN_WORD_LEN          = PREFIX + "minwl";
  public final static String MAX_WORD_LEN          = PREFIX + "maxwl";
  public final static String MAX_QUERY_TERMS       = PREFIX + "maxqt";
  public final static String MAX_NUM_TOKENS_PARSED = PREFIX + "maxntp";
  public final static String BOOST                 = PREFIX + "boost"; // boost or not?

  // the /mlt request handler uses 'rows'
  public final static String DOC_COUNT = PREFIX + "count";

  // Do you want to include the original document in the results or not
  public final static String MATCH_INCLUDE = PREFIX + "match.include";
  
  // If multiple docs are matched in the query, what offset do you want?
  public final static String MATCH_OFFSET  = PREFIX + "match.offset";

  // Do you want to include the original document in the results or not
  public final static String INTERESTING_TERMS = PREFIX + "interestingTerms";  // false,details,(list or true)
  
  public enum TermStyle {
    NONE,
    LIST,
    DETAILS;
    
    public static TermStyle get( String p )
    {
      if( p != null ) {
        p = p.toUpperCase();
        if( p.equals( "DETAILS" ) ) {
          return DETAILS;
        }
        else if( p.equals( "LIST" ) ) {
          return LIST;
        }
      }
      return NONE; 
    }
  }
}
