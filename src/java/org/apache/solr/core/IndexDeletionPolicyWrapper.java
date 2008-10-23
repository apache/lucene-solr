package org.apache.solr.core;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A wrapper for an IndexDeletionPolicy instance.
 * <p/>
 * Provides features for looking up IndexCommit given a version. Allows reserving index
 * commit points for certain amounts of time to support features such as index replication
 * or snapshooting directly out of a live index directory.
 *
 * @version $Id$
 * @see org.apache.lucene.index.IndexDeletionPolicy
 */
public class IndexDeletionPolicyWrapper implements IndexDeletionPolicy {
  private IndexDeletionPolicy deletionPolicy;
  private Map<Long, IndexCommit> solrVersionVsCommits = new ConcurrentHashMap<Long, IndexCommit>();
  private Map<Long, Long> reserves = new ConcurrentHashMap<Long,Long>();
  private IndexCommit latestCommit;

  public IndexDeletionPolicyWrapper(IndexDeletionPolicy deletionPolicy) {
    this.deletionPolicy = deletionPolicy;
  }

  /**
   * Gets the most recent commit point
   * <p/>
   * It is recommended to reserve a commit point for the duration of usage so that
   * it is not deleted by the underlying deletion policy
   *
   * @return the most recent commit point
   */
  public IndexCommit getLatestCommit() {
    return latestCommit;
  }

  public IndexDeletionPolicy getWrappedDeletionPolicy() {
    return deletionPolicy;
  }

  /**
   * Set the duration for which commit point is to be reserved by the deletion policy.
   *
   * @param indexVersion version of the commit point to be reserved
   * @param reserveTime  time in milliseconds for which the commit point is to be reserved
   */
  public void setReserveDuration(Long indexVersion, long reserveTime) {
      reserves.put(indexVersion, System.currentTimeMillis() + reserveTime);
  }

  private void cleanReserves() {
    long currentTime = System.currentTimeMillis();
    for (Map.Entry<Long, Long> entry : reserves.entrySet()) {
      if (entry.getValue() < currentTime) {
        reserves.remove(entry.getKey());
      }
    }
  }

  private List<IndexCommitWrapper> wrap(List<IndexCommit> list) {
    List<IndexCommitWrapper> result = new ArrayList<IndexCommitWrapper>();
    for (IndexCommit indexCommit : list) result.add(new IndexCommitWrapper(indexCommit));
    return result;
  }

  /**
   * Internal use for Lucene... do not explicitly call.
   */
  public void onInit(List list) throws IOException {
    List<IndexCommitWrapper> wrapperList = wrap(list);
    deletionPolicy.onInit(wrapperList);
    updateCommitPoints(wrapperList);
    cleanReserves();
  }

  /**
   * Internal use for Lucene... do not explicitly call.
   */
  public void onCommit(List list) throws IOException {
    List<IndexCommitWrapper> wrapperList = wrap(list);
    deletionPolicy.onCommit(wrapperList);
    updateCommitPoints(wrapperList);
    cleanReserves();
  }

  private class IndexCommitWrapper extends IndexCommit {
    IndexCommit delegate;

    IndexCommitWrapper(IndexCommit delegate) {
      this.delegate = delegate;
    }

    public String getSegmentsFileName() {
      return delegate.getSegmentsFileName();
    }

    public Collection getFileNames() throws IOException {
      return delegate.getFileNames();
    }

    public Directory getDirectory() {
      return delegate.getDirectory();
    }

    public void delete() {
      Long reserve = reserves.get(delegate.getVersion());
      if (reserve != null && System.currentTimeMillis() < reserve) return;
      delegate.delete();
    }

    public boolean isOptimized() {
      return delegate.isOptimized();
    }

    public boolean equals(Object o) {
      return delegate.equals(o);
    }

    public int hashCode() {
      return delegate.hashCode();
    }

    public long getVersion() {
      return delegate.getVersion();
    }

    public long getGeneration() {
      return delegate.getGeneration();
    }

    public boolean isDeleted() {
      return delegate.isDeleted();
    }

    public long getTimestamp() throws IOException {
      return delegate.getTimestamp();
    }
  }

  /**
   * @param version the version of the commit point
   * @return a commit point corresponding to the given version
   */
  public IndexCommit getCommitPoint(Long version) {
    return solrVersionVsCommits.get(version);
  }

  /**
   * Gets the commit points for the index.
   * This map instance may change between commits and commit points may be deleted.
   * It is recommended to reserve a commit point for the duration of usage
   *
   * @return a Map of version to commit points
   */
  public Map<Long, IndexCommit> getCommits() {
    return solrVersionVsCommits;
  }

  private void updateCommitPoints(List<IndexCommitWrapper> list) {
    Map<Long, IndexCommit> map = new ConcurrentHashMap<Long, IndexCommit>();
    for (IndexCommitWrapper wrapper : list) {
      if (!wrapper.isDeleted())
        map.put(wrapper.getVersion(), wrapper.delegate);
    }
    solrVersionVsCommits = map;
    latestCommit = ((list.get(list.size() - 1)).delegate);
  }
}

