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

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.IOUtils;
import org.apache.solr.common.util.XMLErrorLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

// immutable
public class CloudState {
	protected static Logger log = LoggerFactory.getLogger(CloudState.class);
	private static final XMLErrorLogger xmllog = new XMLErrorLogger(log);
	private Map<String, Map<String, Slice>> collectionStates;
	private Set<String> liveNodes;

	public CloudState() {
		this.liveNodes = new HashSet<String>();
		this.collectionStates = new HashMap<String, Map<String, Slice>>(0);
	}

	public CloudState(Set<String> liveNodes,
			Map<String, Map<String, Slice>> collectionStates) {
		this.liveNodes = liveNodes;
		this.collectionStates = collectionStates;
	}

	public Slice getSlice(String collection, String slice) {
		if (collectionStates.containsKey(collection)
				&& collectionStates.get(collection).containsKey(slice))
			return collectionStates.get(collection).get(slice);
		return null;
	}

	public void addSlice(String collection, Slice slice) {
		if (!collectionStates.containsKey(collection)) {
			log.info("New collection");
			collectionStates.put(collection, new HashMap<String, Slice>());
		}
		if (!collectionStates.get(collection).containsKey(slice.getName())) {
			log.info("New slice: " + slice.getName());
			collectionStates.get(collection).put(slice.getName(), slice);
		} else {
			log.info("Updating existing slice");
			
			Map<String, ZkNodeProps> shards = new HashMap<String, ZkNodeProps>();
			
			Slice existingSlice = collectionStates.get(collection).get(slice.getName());
			shards.putAll(existingSlice.getShards());
			shards.putAll(slice.getShards());
			Slice updatedSlice = new Slice(slice.getName(), shards);
			collectionStates.get(collection).put(slice.getName(), updatedSlice);
		}
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

	public void setLiveNodes(Set<String> liveNodes) {
		this.liveNodes = liveNodes;
	}

	public boolean liveNodesContain(String name) {
		return liveNodes.contains(name);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("live nodes:" + liveNodes);
		sb.append(" collections:" + collectionStates);
		return sb.toString();
	}

	public static CloudState load(byte[] state) {
		// TODO this should throw some exception instead of eating them
		CloudState cloudState = new CloudState();
		if(state != null && state.length > 0) {
			InputSource is = new InputSource(new ByteArrayInputStream(state));
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
	
			try {
				DocumentBuilder db = dbf.newDocumentBuilder();
	
				db.setErrorHandler(xmllog);
				Document doc = db.parse(is);
	
				Element root = doc.getDocumentElement();
	
				NodeList collectionStates = root.getChildNodes();
				for (int x = 0; x < collectionStates.getLength(); x++) {
					Node collectionState = collectionStates.item(x);
					String collectionName = collectionState.getAttributes()
							.getNamedItem("name").getNodeValue();
					NodeList slices = collectionState.getChildNodes();
					for (int y = 0; y < slices.getLength(); y++) {
						Node slice = slices.item(y);
						Node sliceName = slice.getAttributes().getNamedItem("name");
						
						NodeList shardsNodeList = slice.getChildNodes();
						Map<String, ZkNodeProps> shards = new HashMap<String, ZkNodeProps>();
						for (int z = 0; z < shardsNodeList.getLength(); z++) {
							Node shard = shardsNodeList.item(z);
							String shardName = shard.getAttributes()
									.getNamedItem("name").getNodeValue();
							NodeList propsList = shard.getChildNodes();
							ZkNodeProps props = new ZkNodeProps();
							
							for (int i = 0; i < propsList.getLength(); i++) {
								Node prop = propsList.item(i);
								String propName = prop.getAttributes()
										.getNamedItem("name").getNodeValue();
								String propValue = prop.getTextContent();
								props.put(propName, propValue);
							}
							shards.put(shardName, props);
						}
						Slice s = new Slice(sliceName.getNodeValue(), shards);
						cloudState.addSlice(collectionName, s);
					}
				}
			} catch (SAXException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ParserConfigurationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				// some XML parsers are broken and don't close the byte stream (but
				// they should according to spec)
				IOUtils.closeQuietly(is.getByteStream());
			}
		}
		return cloudState;
	}

	public static byte[] store(CloudState state)
			throws UnsupportedEncodingException, IOException {
		StringWriter stringWriter = new StringWriter();
		Writer w = new BufferedWriter(stringWriter);
		w.write("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n");
		w.write("<clusterstate>");
		Map<String, Map<String, Slice>> collectionStates = state
				.getCollectionStates();
		for (String collectionName : collectionStates.keySet()) {
			w.write("<collectionstate name=\"" + collectionName + "\">");
			Map<String, Slice> collection = collectionStates
					.get(collectionName);
			for (String sliceName : collection.keySet()) {
				w.write("<shard name=\"" + sliceName + "\">");
				Slice slice = collection.get(sliceName);
				Map<String, ZkNodeProps> shards = slice.getShards();
				for (String shardName : shards.keySet()) {
					w.write("<replica name=\"" + shardName + "\">");
					ZkNodeProps props = shards.get(shardName);
					for (String propName : props.keySet()) {
						w.write("<str name=\"" + propName + "\">"
								+ props.get(propName) + "</str>");
					}
					w.write("</replica>");

				}
				w.write("</shard>");
			}
			w.write("</collectionstate>");
		}
		w.write("</clusterstate>");
		w.flush();
		w.close();
		return stringWriter.toString().getBytes("UTF-8");

	}

  public void setLiveNodes(List<String> liveNodes) {
    Set<String> liveNodesSet = new HashSet<String>(liveNodes.size());
    liveNodesSet.addAll(liveNodes);
    this.liveNodes = liveNodesSet;
  }
}
