package org.apache.solr.cluster.api;

import org.apache.solr.common.SolrException;

import java.io.IOException;
import java.io.InputStream;

/**A binary resource. The system is agnostic of the content type */
public interface Resource {
    /** read a file/resource.
     * The caller should consume the stream completely and should not hold a reference to this stream.
     * This method closes the stream soon after the method returns
     * @param resourceConsumer This should be a full path. e.g schema.xml , solrconfig.xml , lang/stopwords.txt etc
     */
    void get(Consumer resourceConsumer) throws SolrException;

    interface Consumer {
        void read(InputStream is) throws IOException;
    }
}
