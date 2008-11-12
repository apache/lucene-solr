package org.apache.solr.common.params;


/**
 *
 *
 **/
public interface TermVectorParams {

  public static final String TV_PREFIX = "tv.";

    /**
  * Return Term Frequency info
  * */
  public static final String TF =  TV_PREFIX + "tf";
  /**
  * Return Term Vector position information
  *
  * */
  public static final String POSITIONS = TV_PREFIX + "positions";
  /**
  * Return offset information, if available
  * */
  public static final String OFFSETS = TV_PREFIX + "offsets";
  /**
  * Return IDF information.  May be expensive
  * */
  public static final String DF = TV_PREFIX + "df";

  /**
   * Return TF-IDF calculation, i.e. (tf / idf).  May be expensive.
   */
  public static final String TF_IDF = TV_PREFIX + "tf_idf";


  /**
   * Return all the options: TF, positions, offsets, idf
   */
  public static final String ALL = TV_PREFIX + "all";

  /**
   * The fields to get term vectors for
   */
  public static final String FIELDS = TV_PREFIX + "fl";

  /**
   * The Doc Ids (Lucene internal ids) of the docs to get the term vectors for
   */
  public static final String DOC_IDS = TV_PREFIX + "docIds";
}
