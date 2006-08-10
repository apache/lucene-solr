/** 
 * Copyright 2004 The Apache Software Foundation 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License. 
 */
package org.apache.lucene.gdata.server.registry.configuration;

import junit.framework.TestCase;

public class TestPropertyInjector extends TestCase {
    private PropertyInjector injector;

    protected void setUp() throws Exception {
        super.setUp();
        this.injector = new PropertyInjector();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.server.registry.configuration.PropertyInjector.setTargetObject(Object)'
     */
    public void testSetTargetObject() {
        try {
            this.injector.setTargetObject(null);
            fail("must not be null");
        } catch (IllegalArgumentException e) {
            // TODO: handle exception
        }
        try {
            this.injector.setTargetObject(new Object());
            fail("no getter or setter methodes");
        } catch (InjectionException e) {
            // TODO: handle exception
        }
      
       this.injector.setTargetObject(new TestBean());
       assertEquals(1,this.injector.getOptionalSize());
       assertEquals(1,this.injector.getRequiredSize());
    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.server.registry.configuration.PropertyInjector.injectProperties(ConfigurationBean)'
     */
    public void testInjectProperties() throws Exception {
        ComponentConfiguration bean = new ComponentConfiguration();
        bean.set("someValue","bla");
        try{
            this.injector.injectProperties(bean);
            fail("target is not set");
            }catch (IllegalStateException e) {
          
            }
        TestBean testBean = new TestBean();
        this.injector.setTargetObject(testBean);
        try{
            this.injector.injectProperties(null);
            fail("object is null");
            }catch (IllegalArgumentException e) {
          
            }
        try{
        this.injector.injectProperties(bean);
        fail("requiered Property is not available in config bean");
        }catch (InjectionException e) {
          
        }
        
        bean.set("test","fooBar");
        bean.set("testClass","java.lang.Object");
        this.injector.injectProperties(bean);
        
        assertEquals("fooBar",testBean.getTest());
        assertEquals(Object.class,testBean.getTestClass());
        
        
        
        this.injector = new PropertyInjector();
        SubTestBean subTestBean = new SubTestBean();
        this.injector.setTargetObject(subTestBean);
        bean.set("number","333");
        this.injector.injectProperties(bean);
        
        assertEquals("fooBar",subTestBean.getTest());
        assertEquals(Object.class,subTestBean.getTestClass());
        assertEquals(333,subTestBean.getNumber());
        
        bean = new ComponentConfiguration();
        bean.set("test","fooBar");
        bean.set("number","333");
        bean.set("wrapper","1.2");
       
        subTestBean = new SubTestBean();
        this.injector.setTargetObject(subTestBean);
        this.injector.injectProperties(bean);
        
        assertEquals("fooBar",subTestBean.getTest());
        assertEquals(333,subTestBean.getNumber());
        assertEquals(new Float(1.2),subTestBean.getWrapper());
    

    }
    
    
   public static class TestBean{
       
        private String test;
        private Class testClass;
        /**
         * @return Returns the test.
         */
        public String getTest() {
            return test;
        }
        /**
         * @param test The test to set.
         */
        @Requiered
        public void setTest(String test) {
            this.test = test;
        }
        /**
         * @return Returns the testClass.
         */
        public Class getTestClass() {
            return testClass;
        }
        /**
         * @param testClass The testClass to set.
         */
        public void setTestClass(Class testClass) {
            this.testClass = testClass;
        }
        
        
    }
   public static class SubTestBean extends TestBean{
      private int number;
      private Float wrapper;

    /**
     * @return Returns the wrapper.
     */
    public Float getWrapper() {
        return wrapper;
    }

    /**
     * @param wrapper The wrapper to set.
     */
    public void setWrapper(Float wrapper) {
        this.wrapper = wrapper;
    }

    /**
     * @return Returns the number.
     */
    
    public int getNumber() {
        return number;
    }

    /**
     * @param number The number to set.
     */
    @Requiered
    public void setNumber(int number) {
        this.number = number;
    }
       
       
       
       
   }
    


}
