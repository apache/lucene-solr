package org.apache.solr.common.cloud;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.noggit.JSONWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.HashPartitioner.Range;
import org.apache.zookeeper.KeeperException;

public class CloudState implements JSONWriter.Writable {
	private final Map<String, Map<String,Slice>> collectionStates;  // Map<collectionName, Map<sliceName,Slice>>
	private final Set<String> liveNodes;
  
  private final HashPartitioner hp = new HashPartitioner();
  
  private final Map<String,RangeInfo> rangeInfos = new HashMap<String,RangeInfo>();
  public Map<String,Map<String,ZkNodeProps>> leaders = new HashMap<String,Map<String,ZkNodeProps>>();


	public CloudState() {
		this.liveNodes = new HashSet<String>();
		this.collectionStates = new HashMap<String,Map<String,Slice>>(0);
		addRangeInfos(collectionStates.keySet());
		getShardLeaders();
	}

	public CloudState(Set<String> liveNodes,
			Map<String, Map<String,Slice>> collectionStates) {
		this.liveNodes = new HashSet<String>(liveNodes.size());
		this.liveNodes.addAll(liveNodes);
		this.collectionStates = new HashMap<String, Map<String,Slice>>(collectionStates.size());
		this.collectionStates.putAll(collectionStates);
		addRangeInfos(collectionStates.keySet());
		getShardLeaders();
	}

	private void getShardLeaders() {
    Set<Entry<String,Map<String,Slice>>> collections = collectionStates.entrySet();
    for (Entry<String,Map<String,Slice>> collection : collections) {
      Map<String,Slice> state = collection.getValue();
      Set<Entry<String,Slice>> slices = state.entrySet();
      for (Entry<String,Slice> sliceEntry : slices) {
        Slice slice = sliceEntry.getValue();
        Map<String,ZkNodeProps> shards = slice.getShards();
        Set<Entry<String,ZkNodeProps>> shardsEntries = shards.entrySet();
        for (Entry<String,ZkNodeProps> shardEntry : shardsEntries) {
          ZkNodeProps props = shardEntry.getValue();
          if (props.containsKey(ZkStateReader.LEADER_PROP)) {
            Map<String,ZkNodeProps> leadersForCollection = leaders.get(collection.getKey());
            if (leadersForCollection == null) {
              leadersForCollection = new HashMap<String,ZkNodeProps>();
        
              leaders.put(collection.getKey(), leadersForCollection);
            }
            leadersForCollection.put(sliceEntry.getKey(), props);
          }
        }
      }
    }
  }
	
	public ZkNodeProps getLeader(String collection, String shard) {
	  Map<String,ZkNodeProps> collectionLeaders = leaders.get(collection);
	  if (collectionLeaders == null) return null;
	  return collectionLeaders.get(shard);
	}

  private void addRangeInfos(Set<String> collections) {
    for (String collection : collections) {
      addRangeInfo(collection);
    }
  }

  public Slice getSlice(String collection, String slice) {
		if (collectionStates.containsKey(collection)
				&& collectionStates.get(collection).containsKey(slice))
			return collectionStates.get(collection).get(slice);
		return null;
	}

	public Map<String, Slice> getSlices(String collection) {
		if(!collectionStates.containsKey(collection))
			return null;
		return Collections.unmodifiableMap(collectionStates.get(collection));
	}

	public Set<String> getCollections() {
		return Collections.unmodifiableSet(collectionStates.keySet());
	}

	public Map<String, Map<String, Slice>> getCollectionStates() {
		return Collections.unmodifiableMap(collectionStates);
	}

	public Set<String> getLiveNodes() {
		return Collections.unmodifiableSet(liveNodes);
	}

	public String getShardId(String coreNodeName) {
	  for (Entry<String, Map<String, Slice>> states: collectionStates.entrySet()){
	    for(Entry<String, Slice> slices: states.getValue().entrySet()) {
	      for(Entry<String, ZkNodeProps> shards: slices.getValue().getShards().entrySet()){
	        if(coreNodeName.equals(shards.getKey())) {
	          return slices.getKey();
	        }
	      }
	    }
	  }
	  return null;
	}

	public boolean liveNodesContain(String name) {
		return liveNodes.contains(name);
	}
	
	public RangeInfo getRanges(String collection) {
    // TODO: store this in zk
    RangeInfo rangeInfo = rangeInfos.get(collection);

	  return rangeInfo;
	}

  private RangeInfo addRangeInfo(String collection) {
    List<Range> ranges;
    RangeInfo rangeInfo;
    rangeInfo = new RangeInfo();

    Map<String,Slice> slices = getSlices(collection);
    
    if (slices == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Can not find collection "
          + collection + " in " + this);
    }
    
    Set<String> shards = slices.keySet();
    ArrayList<String> shardList = new ArrayList<String>(shards.size());
    shardList.addAll(shards);
    Collections.sort(shardList);
    
    ranges = hp.partitionRange(shards.size());
    
    rangeInfo.ranges = ranges;
    rangeInfo.shardList = shardList;
    rangeInfos.put(collection, rangeInfo);
    return rangeInfo;
  }
	
  public String getShard(int hash, String collection) {
    RangeInfo rangInfo = getRanges(collection);
    
    int cnt = 0;
    for (Range range : rangInfo.ranges) {
      if (hash < range.max) {
        return rangInfo.shardList.get(cnt);
      }
      cnt++;
    }
    
    throw new IllegalStateException("The HashPartitioner failed");
  }

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("live nodes:" + liveNodes);
		sb.append(" collections:" + collectionStates);
		return sb.toString();
	}

	public static CloudState load(SolrZkClient zkClient, Set<String> liveNodes) throws KeeperException, InterruptedException {
    byte[] state = zkClient.getData(ZkStateReader.CLUSTER_STATE,
        null, null, true);
    return load(state, liveNodes);
	}
	
	public static CloudState load(byte[] bytes, Set<String> liveNodes) throws KeeperException, InterruptedException {
    if (bytes == null || bytes.length == 0) {
      return new CloudState(liveNodes, Collections.<String, Map<String,Slice>>emptyMap());
    }
    
    LinkedHashMap<String, Object> stateMap = (LinkedHashMap<String, Object>) ZkStateReader.fromJSON(bytes);
    HashMap<String,Map<String, Slice>> state = new HashMap<String,Map<String,Slice>>();

    for(String collectionName: stateMap.keySet()){
      Map<String, Object> collection = (Map<String, Object>)stateMap.get(collectionName);
      Map<String, Slice> slices = new LinkedHashMap<String,Slice>();
      for(String sliceName: collection.keySet()) {
        Map<String, Map<String, String>> sliceMap = (Map<String, Map<String, String>>)collection.get(sliceName);
        Map<String, ZkNodeProps> shards = new LinkedHashMap<String,ZkNodeProps>();
        for(String shardName: sliceMap.keySet()) {
          shards.put(shardName, new ZkNodeProps(sliceMap.get(shardName)));
        }
        Slice slice = new Slice(sliceName, shards);
        slices.put(sliceName, slice);
      }
      state.put(collectionName, slices);
    }
    return new CloudState(liveNodes, state);
	}

  @Override
  public void write(JSONWriter jsonWriter) {
    jsonWriter.write(collectionStates);
  }
  
  class RangeInfo {
    private List<Range> ranges;
    private ArrayList<String> shardList;
  }


}
