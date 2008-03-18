package org.apache.solr.request;

import java.io.Writer;
import java.io.OutputStream;
import java.io.IOException;

/**
 * Implementations of <code>BinaryQueryResponseWriter</code> are used to
 * write response in binary format
 * Functionality is exactly same as its parent class <code>QueryResponseWriter</code
 * But it may not implement the <code>write(Writer writer, SolrQueryRequest request, SolrQueryResponse response)</code>
 * method  
 *
 */
public interface BinaryQueryResponseWriter extends QueryResponseWriter{

    /**Use it to write the reponse in a binary format
     */
    public void write(OutputStream out, SolrQueryRequest request, SolrQueryResponse response) throws IOException;
}
