package org.apache.solr.update;

import org.apache.lucene.util.BytesRef;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.plugin.PluginInfoInitialized;

public abstract class UpdateLog implements PluginInfoInitialized {
  public static final int ADD = 0x00;
  public static final int DELETE = 0x01;
  public static final int DELETE_BY_QUERY = 0x02;

  public abstract void init(UpdateHandler uhandler, SolrCore core);
  public abstract void add(AddUpdateCommand cmd);
  public abstract void delete(DeleteUpdateCommand cmd);
  public abstract void deleteByQuery(DeleteUpdateCommand cmd);
  public abstract void preCommit(CommitUpdateCommand cmd);
  public abstract void postCommit(CommitUpdateCommand cmd);
  public abstract void preSoftCommit(CommitUpdateCommand cmd);
  public abstract void postSoftCommit(CommitUpdateCommand cmd);
  public abstract Object lookup(BytesRef indexedId);
  public abstract void close();
}
