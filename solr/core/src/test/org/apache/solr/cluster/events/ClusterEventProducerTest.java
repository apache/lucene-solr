package org.apache.solr.cluster.events;

import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class ClusterEventProducerTest extends SolrCloudTestCase {

  public static class AllEventsListener implements ClusterEventListener {
    Map<ClusterEvent.EventType, List<ClusterEvent>> events = new HashMap<>();

    @Override
    public Set<ClusterEvent.EventType> getEventTypes() {
      return new HashSet<>(Arrays.asList(ClusterEvent.EventType.values()));
    }

    @Override
    public void onEvent(ClusterEvent event) {
      events.computeIfAbsent(event.getType(), type -> new ArrayList<>()).add(event);
    }
  }

  private static AllEventsListener eventsListener = new AllEventsListener();

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(3)
        .addConfig("conf", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
    cluster.getOpenOverseer().getCoreContainer().getClusterEventProducer().registerListener(eventsListener);
  }

  @Before
  public void setUp() throws Exception  {
    super.setUp();
    cluster.deleteAllCollections();
  }

  @Test
  public void testNodesEvent() throws Exception {

    // NODES_DOWN

    // don't kill Overseer
    JettySolrRunner nonOverseerJetty = null;
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      if (cluster.getOpenOverseer().getCoreContainer().getZkController().getNodeName().equals(jetty.getNodeName())) {
        continue;
      }
      nonOverseerJetty = jetty;
      break;
    }
    String nodeName = nonOverseerJetty.getNodeName();
    cluster.stopJettySolrRunner(nonOverseerJetty);
    cluster.waitForJettyToStop(nonOverseerJetty);
    assertNotNull("should be NODES_DOWN events", eventsListener.events.get(ClusterEvent.EventType.NODES_DOWN));
    List<ClusterEvent> events = eventsListener.events.get(ClusterEvent.EventType.NODES_DOWN);
    assertEquals("should be one NODES_DOWN event", 1, events.size());
    ClusterEvent event = events.get(0);
    assertEquals("should be NODES_DOWN event type", ClusterEvent.EventType.NODES_DOWN, event.getType());
    NodesDownEvent nodesDown = (NodesDownEvent) event;
    assertEquals("should be node " + nodeName, nodeName, nodesDown.getNodeNames().iterator().next());

    // NODES_UP
    JettySolrRunner newNode = cluster.startJettySolrRunner();
    cluster.waitForNode(newNode, 60);
    assertNotNull("should be NODES_UP events", eventsListener.events.get(ClusterEvent.EventType.NODES_UP));
    events = eventsListener.events.get(ClusterEvent.EventType.NODES_UP);
    assertEquals("should be one NODES_UP event", 1, events.size());
    event = events.get(0);
    assertEquals("should be NODES_UP event type", ClusterEvent.EventType.NODES_UP, event.getType());
    NodesUpEvent nodesUp = (NodesUpEvent) event;
    assertEquals("should be node " + newNode.getNodeName(), newNode.getNodeName(), nodesUp.getNodeNames().iterator().next());


  }
}
