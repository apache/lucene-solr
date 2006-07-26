package org.apache.lucene.gdata.server.registry.configuration;

import junit.framework.TestCase;

public class TestConfigurationBean extends TestCase {
    private ComponentConfiguration bean;

    protected void setUp() throws Exception {
        super.setUp();
        this.bean = new ComponentConfiguration();

    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.server.registry.configuration.ConfigurationBean.set(String,
     * String)'
     */
    public void testSet() {
        this.bean.set("field", "value");
        try {
            this.bean.set("field", "value");
            fail("field already set");
        } catch (IllegalArgumentException e) {
            //
        }
        assertEquals("value", this.bean.get("field"));
    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.server.registry.configuration.ConfigurationBean.get(String)'
     */
    public void testGet() {
        assertNull(this.bean.get("field"));
        this.bean.set("field", "value");
        assertEquals("value", this.bean.get("field"));
    }

}
