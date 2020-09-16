package org.apache.solr.handler;

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.api.Command;
import org.apache.solr.api.EndPoint;
import org.apache.solr.api.PayloadObj;
import org.apache.solr.client.solrj.request.beans.ClusterPropInfo;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.params.DefaultSolrParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.ReflectMapWriter;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.DELETE;
import static org.apache.solr.client.solrj.SolrRequest.METHOD.GET;
import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDROLE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CLUSTERPROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.OVERSEERSTATUS;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.REMOVEROLE;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_READ_PERM;

public class ClusterAPI {
  private final CoreContainer coreContainer;

  public  final Commands commands = new Commands();

  public ClusterAPI(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
  }


  @EndPoint(method = GET,
      path = "/cluster/overseer",
      permission = COLL_READ_PERM)
  public void getOverseerStatus(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    coreContainer.getCollectionsHandler().handleRequestBody(wrapParams(req, "action", OVERSEERSTATUS.toString()), rsp);
  }

  @EndPoint(method = GET,
      path = "/cluster",
      permission = COLL_READ_PERM)
  public void getCluster(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    CollectionsHandler.CollectionOperation.LIST_OP.execute(req, rsp, coreContainer.getCollectionsHandler());
  }

  @EndPoint(method = DELETE,
      path = "/cluster/command-status/{id}",
      permission = COLL_EDIT_PERM)
  public void deleteCommandStatus(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    wrapParams(req);
    CollectionsHandler.CollectionOperation.DELETESTATUS_OP.execute(req, rsp, coreContainer.getCollectionsHandler());
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public static SolrQueryRequest wrapParams(SolrQueryRequest req, Object... def) {
    Map m = Utils.makeMap(def);
    return wrapParams(req, m);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static SolrQueryRequest wrapParams(SolrQueryRequest req, Map m) {
    DefaultSolrParams dsp = new DefaultSolrParams(req.getParams(), new MapSolrParams(m));
    req.setParams(dsp);
    return req;
  }

  @EndPoint(method = GET,
      path = "/cluster/command-status",
      permission = COLL_READ_PERM)
  public void getCommandStatus(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    CollectionsHandler.CollectionOperation.REQUESTSTATUS_OP.execute(req, rsp, coreContainer.getCollectionsHandler());
  }

  @EndPoint(method = GET,
      path = "/cluster/nodes",
      permission = COLL_READ_PERM)
  public void getNodes(SolrQueryRequest req, SolrQueryResponse rsp) {
    rsp.add("nodes", coreContainer.getZkController().getClusterState().getLiveNodes());
  }

  @EndPoint(method = POST,
      path = "/cluster",
      permission = COLL_EDIT_PERM)
  public class Commands {


    @Command(name = "add-role")
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void addRole(PayloadObj<RoleInfo> obj) throws Exception {
      RoleInfo info = obj.get();
      Map m = info.toMap(new HashMap<>());
      m.put("action", ADDROLE.toString());
      coreContainer.getCollectionsHandler().handleRequestBody(wrapParams(obj.getRequest(), m), obj.getResponse());
    }

    @Command(name = "remove-role")
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void removeRole(PayloadObj<RoleInfo> obj) throws Exception {
      RoleInfo info = obj.get();
      Map m = info.toMap(new HashMap<>());
      m.put("action", REMOVEROLE.toString());
      coreContainer.getCollectionsHandler().handleRequestBody(wrapParams(obj.getRequest(), info.toMap(new HashMap<>())), obj.getResponse());

    }

    @Command(name = "set-obj-property")
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void setObjProperty(PayloadObj<ClusterPropInfo> obj) {
      //Not using the object directly here because the API differentiate between {name:null} and {}
      Map m = obj.getDataMap();
      ClusterProperties clusterProperties = new ClusterProperties(coreContainer.getZkController().getZkClient());
      try {
        clusterProperties.setClusterProperties(m);
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error in API", e);
      }

    }

    @Command(name = "set-property")
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void setProperty(PayloadObj<Map<String,String>> obj) throws Exception {
      Map m =  obj.get();
      m.put("action", CLUSTERPROP.toString());
      coreContainer.getCollectionsHandler().handleRequestBody(wrapParams(obj.getRequest(),m ), obj.getResponse());

    }

  }

  public static class RoleInfo implements ReflectMapWriter {
    @JsonProperty(required = true)
    public String node;
    @JsonProperty(required = true)
    public String role;

  }


}
