package org.apache.solr.handler.dataimport;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;

public class DebugInfo {
  public List<SolrInputDocument> debugDocuments = new ArrayList<SolrInputDocument>(0);
  public NamedList<String> debugVerboseOutput = null;
  public boolean verbose;
  
  public DebugInfo(Map<String,Object> requestParams) {
    verbose = StrUtils.parseBool((String) requestParams.get("verbose"), false);
    debugVerboseOutput = new NamedList<String>();
  }
}
