package org.apache.solr.store.blob.util;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
//import org.apache.solr.client.solrj.request.*;
//import org.apache.solr.client.solrj.response.UpdateResponse;
//import com.google.common.base.Joiner;
//import org.apache.solr.common.params.ModifiableSolrParams;
//import org.apache.solr.common.util.ContentStreamBase;
//import org.junit.Assert;
//import searchserver.SfdcUserData;
//import searchserver.analysis.Language;
//import searchserver.handler.SynonymDataHandler;
//import searchserver.update.processor.SfdcMetadataProcessorFactory;

import java.util.List;

/**
 * Utility methods for blob store related tests.
 *
 * @author iginzburg
 * @since 214/solr.6
 */
public class BlobTestUtil {
    /**
     * Indexes the list of docs on the provided server, while verifying the core is at (or above) the expected (<code>lastAck</code>)
     * sequence number, and by setting its sequence number post successful indexing to <code>sequenceNumber</code>.<p>
     * Note that if the core rejects the indexing batch (because it is at the wrong sequence number) there will <b>not</b> be
     * an exception thrown here or anything else (and I wasn't able to find the rejection logs in the console either). Tests
     * should be checking success explicitly (query the core, verify local core sequence number, the number of files composing
     * the core's segments etc).<p>
     *
     * Note that for the first indexing batch on an empty core, <code>lastAck</code> can be set to 0L or 1L, both work.
     * On subsequent batches, it must be set to a value equal to or lesser than the sequence number set in the previous successful
     * indexing batch.
     */
    static public void addDocsAndCommit(HttpSolrClient server, long sequenceNumber, long lastAck, List<SolrInputDocument> docs) throws Exception {
//        // Create parameters and set the passed in lask ack and sequence number
//        ModifiableSolrParams params = new ModifiableSolrParams();
//        // We (still) need to pass both REPLAY_COUNT and SEQUENCE_NUMBER as parameters. The clean up of removing REPLAY_COUNT
//        // or at least making it optional did not occur yet in Solr.
//        params.add(SfdcMetadataProcessorFactory.SEQUENCE_NUMBER, Long.toString(sequenceNumber));
//        params.add(SfdcUserData.REPLAY_COUNT, Long.toString(sequenceNumber));
//        // Assuming both LAST_ACKNOWLEDGED and OLD_REPLAY_COUNT are needed as well.
//        params.add(SfdcMetadataProcessorFactory.LAST_ACKNOWLEDGED, Long.toString(lastAck));
//        params.add(SfdcMetadataProcessorFactory.OLD_REPLAY_COUNT, Long.toString(lastAck));
//
//        // Create an update request, add the passed in docs, set the parameters and tell it to commit
//        UpdateRequest updateRequest = new UpdateRequest();
//        for (SolrInputDocument doc : docs) {
//            updateRequest.add(doc);
//        }
//        updateRequest.setParams(params);
//        updateRequest.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
//
//        // Do the work
//        UpdateResponse response = updateRequest.process(server);
//        Assert.assertEquals("Update request to server failed", 0, response.getStatus());
    }

    /**
     * Update synonyms for english language
     */
    static public void updateSynonym(HttpSolrClient server, String... synonyms) throws Exception {
//        ContentStreamUpdateRequest request = new ContentStreamUpdateRequest(SynonymDataHandler.SYNONYM_DATA_HANDLER_PATH);
//        request.addContentStream(new ContentStreamBase.StringStream(Joiner.on(",").join(synonyms)));
//        request.setParam(SynonymDataHandler.LANGUAGE_PARAM, Language.ENGLISH.getCode());
//        server.request(request);
    }
    
    static public String getValidSolrCoreName() {
      return "sLuigi_00De00000000000_0FSe00000000000_0FVe00000000000";
    }
}
