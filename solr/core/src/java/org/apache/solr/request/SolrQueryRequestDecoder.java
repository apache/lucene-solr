package org.apache.solr.request;


import org.apache.solr.core.SolrCore;

/**
 * Given some raw input (such as an HttpRequest), create a SolrQueryRequest
 *
 **/
public interface SolrQueryRequestDecoder {

  /**
   *
   * @return The Decoded SolrQueryRequest
   * @throws Exception
   */
  SolrQueryRequest decode() throws Exception;

  /**
   * Decode in the context of a {@link org.apache.solr.core.SolrCore}.
   * @param core The core
   * @return the {@link org.apache.solr.request.SolrQueryRequest}
   * @throws Exception
   */
  SolrQueryRequest decode(SolrCore core) throws Exception;
}
