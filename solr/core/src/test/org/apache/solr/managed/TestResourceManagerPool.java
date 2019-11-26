package org.apache.solr.managed;

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrResourceLoader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TestResourceManagerPool extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int SPEED = 50;
  private static CountDownLatch manageStartLatch = new CountDownLatch(1);
  private static CountDownLatch manageFinishLatch = new CountDownLatch(1);

  private ResourceManager resourceManager;
  private SolrResourceLoader loader;

  public static class TestComponent implements ManagedComponent {
    SolrResourceContext context;
    ManagedComponentId id;
    int foo, bar, baz;

    public TestComponent(String id) {
      this.id = ManagedComponentId.of(id);
    }

    public int getFoo() {
      return foo;
    }

    public int getBar() {
      return bar;
    }

    public int getBaz() {
      return baz;
    }

    public void setFoo(int foo) {
      this.foo = foo;
      this.bar = foo + 1;
      this.baz = foo + 2;
    }

    @Override
    public ManagedComponentId getManagedComponentId() {
      return id;
    }

    @Override
    public void initializeManagedComponent(ResourceManager resourceManager, String poolName, String... otherPools) {
      context = new SolrResourceContext(resourceManager, this, poolName, otherPools);
    }

    @Override
    public SolrResourceContext getSolrResourceContext() {
      return context;
    }
  }

  public static class MockManagerPool extends ResourceManagerPool<TestComponent> {
    public MockManagerPool(String name, String type, ResourceManager resourceManager, Map<String, Object> poolLimits, Map<String, Object> poolParams) {
      super(name, type, resourceManager, poolLimits, poolParams);
    }

    @Override
    public String getType() {
      return "foo";
    }

    @Override
    public Map<String, Object> getMonitoredValues(TestComponent component) throws Exception {
      Map<String, Object> result = new HashMap<>();
      result.put("bar", component.getBar());
      result.put("baz", component.getBaz());
      result.put("foo", component.getFoo());
      return result;
    }

    @Override
    public Object doSetResourceLimit(TestComponent component, String limitName, Object value) throws Exception {
      if (limitName.equals("foo") && value instanceof Number) {
        component.setFoo(((Number)value).intValue());
        return ((Number)value).intValue();
      } else {
        throw new Exception("invalid limit name or value");
      }
    }

    @Override
    public Map<String, Object> getResourceLimits(TestComponent component) throws Exception {
      return Collections.singletonMap("foo", component.getFoo());
    }

    @Override
    public void doManage() throws Exception {
      if (manageStartLatch.getCount() == 0) { // already fired
        return;
      }
      manageStartLatch.countDown();
      log.info("-- managing");
      Map<String, Map<String, Object>> currentValues = getCurrentValues();
      Map<String, Object> totalValues = aggregateTotalValues(currentValues);
      if (poolLimits.containsKey("foo")) {
        // manage
        if (totalValues.containsKey("bar")) {
          int totalValue = ((Number)totalValues.get("bar")).intValue();
          int poolLimit = ((Number)poolLimits.get("foo")).intValue();
          if (totalValue > poolLimit) {
            for (Object cmp : getComponents().values()) {
              TestComponent component = (TestComponent)cmp;
              int foo = component.getFoo();
              if (foo > 0) {
                component.setFoo(--foo);
              }
            }
          }
        }
      }
      manageFinishLatch.countDown();
    }
  }

  @Before
  public void initManager() {
    loader = new SolrResourceLoader(TEST_PATH());
    resourceManager = new DefaultResourceManager(loader, TimeSource.get("simTime:" + SPEED));
    Map<String, Object> initArgs = new HashMap<>();
    Map<String, Object> config = new HashMap<>();
    initArgs.put("plugins", config);
    Map<String, String> plugins = new HashMap<>();
    Map<String, String> components = new HashMap<>();
    config.put(DefaultResourceManagerPoolFactory.TYPE_TO_POOL, plugins);
    config.put(DefaultResourceManagerPoolFactory.TYPE_TO_COMPONENT, components);
    plugins.put("mock", MockManagerPool.class.getName());
    components.put("mock", TestComponent.class.getName());
    resourceManager.init(new PluginInfo("resourceManager", initArgs));
  }

  @After
  public void destroyManager() {
    if (resourceManager != null) {
      IOUtils.closeQuietly(resourceManager);
      resourceManager = null;
    }
  }

  @Test
  public void testBasic() throws Exception {
    // let it run
    manageStartLatch.countDown();

    resourceManager.createPool("test", "mock", Collections.singletonMap("foo", 10), Collections.emptyMap());
    assertNotNull(resourceManager.getPool("test"));
    for (int i = 0; i < 10; i++) {
      TestComponent component = new TestComponent("component:" + i);
      component.setFoo(i);
      resourceManager.registerComponent("test", component);
    }
    ResourceManagerPool pool = resourceManager.getPool("test");
    assertEquals(10, pool.getComponents().size());
    Map<String, Map<String, Object>> currentValues = pool.getCurrentValues();
    Map<String, Object> totalValues = pool.aggregateTotalValues(currentValues);
    assertNotNull(totalValues.get("bar"));
    assertEquals(55, ((Number)totalValues.get("bar")).intValue());
    assertNotNull(totalValues.get("baz"));
    assertEquals(65, ((Number)totalValues.get("baz")).intValue());
    for (Object cmp : pool.getComponents().values()) {
      TestComponent component = (TestComponent)cmp;
      Map<String, Object> limits = pool.getResourceLimits(component);
      assertEquals(1, limits.size());
      assertNotNull(limits.get("foo"));
      String name = component.getManagedComponentId().getName();
      int val = Integer.parseInt(name);
      assertEquals("foo", val, component.getFoo());
      assertEquals("bar", val + 1, component.getBar());
      assertEquals("baz", val + 2, component.getBaz());
    }
    // set pool limits
    resourceManager.setPoolLimits("test", Collections.singletonMap("foo", 50));
    manageFinishLatch = new CountDownLatch(1);
    manageStartLatch = new CountDownLatch(1);
    boolean await = manageFinishLatch.await(30000 / SPEED, TimeUnit.MILLISECONDS);
    assertTrue("did not finish in time", await);
    currentValues = pool.getCurrentValues();
    totalValues = pool.aggregateTotalValues(currentValues);
    assertNotNull(totalValues.get("bar"));
    assertEquals(46, ((Number)totalValues.get("bar")).intValue());
    assertNotNull(totalValues.get("baz"));
    assertEquals(56, ((Number)totalValues.get("baz")).intValue());
    int changed = 0;
    for (Object cmp : pool.getComponents().values()) {
      TestComponent component = (TestComponent)cmp;
      Map<String, Object> limits = pool.getResourceLimits(component);
      assertEquals(1, limits.size());
      assertNotNull(limits.get("foo"));
      String name = component.getManagedComponentId().getName();
      int val = Integer.parseInt(name);
      if (val > component.getFoo()) {
        changed++;
      }
    }
    assertTrue(changed > 0);
  }
}
