package org.apache.solr.handler;

import org.apache.solr.api.EndPoint;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.DELETE;
import static org.apache.solr.client.solrj.SolrRequest.METHOD.GET;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_READ_PERM;

/**
 * All V2 APIs for collection management
 *
 */
public class CollectionsAPI {

  private final CollectionsHandler collectionsHandler;

  public CollectionsAPI(CollectionsHandler collectionsHandler) {
    this.collectionsHandler = collectionsHandler;
  }

  @EndPoint(
      path = {"/c", "/collections"},
      method = GET,
      permission = COLL_READ_PERM)
  public void getCollections(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    CollectionsHandler.CollectionOperation.LIST_OP.execute(req, rsp, collectionsHandler);
  }

  @EndPoint(path = {"/c/{collection}", "/collections/{collection}"},
      method = DELETE,
      permission = COLL_EDIT_PERM)
  public void deleteCollection(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    req = ClusterAPI.wrapParams(req, "action",
        CollectionAction.DELETE.toString(),
        NAME, req.getPathTemplateValues().get(ZkStateReader.COLLECTION_PROP));
    collectionsHandler.handleRequestBody(req, rsp);
  }

}
