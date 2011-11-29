package org.apache.solr.cloud;

public abstract class ElectionContext {
  
  final String electionPath;
  final byte[] leaderProps;
  final String id;
  
  public ElectionContext(final String shardZkNodeName,
      final String electionPath, final byte[] leaderProps) {
    this.id = shardZkNodeName;
    this.electionPath = electionPath;
    this.leaderProps = leaderProps;
  }
  
}

final class ShardLeaderElectionContext extends ElectionContext {
  
  public ShardLeaderElectionContext(final String shardid,
      final String collection, final String shardZkNodeName, final byte[] props) {
    super(shardZkNodeName, "/collections/" + collection + "/leader_elect/"
        + shardid, props);
  }
  
}

final class OverseerElectionContext extends ElectionContext {
  
  public OverseerElectionContext(final String zkNodeName) {
    super(zkNodeName, "/overseer_elect", null);
  }
  
}
