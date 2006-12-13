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

package org.apache.lucene.gdata.server.registry;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This Listener creates the
 * {@link org.apache.lucene.gdata.server.registry.GDataServerRegistry} when the
 * context is loaded. The registry will be loaded before the
 * {@link org.apache.lucene.gdata.servlet.RequestControllerServlet} is loaded.
 * The Registry will be loaded and set up before the REST interface is available.
 * <p>
 * This ContextListener has to be configured in the <code>web.xml</code>
 * deployment descriptor.
 * </p>
 * <p>
 * When the
 * {@link javax.servlet.ServletContextListener#contextDestroyed(javax.servlet.ServletContextEvent)}
 * method is called the registry will be destroyed using
 * {@link org.apache.lucene.gdata.server.registry.GDataServerRegistry#destroy()}
 * method.
 * 
 * 
 * @author Simon Willnauer
 * 
 */
public class RegistryContextListener implements ServletContextListener {
    private GDataServerRegistry serverRegistry;

    private static final Log LOG = LogFactory
            .getLog(RegistryContextListener.class);

    /**
     * @see javax.servlet.ServletContextListener#contextInitialized(javax.servlet.ServletContextEvent)
     */
    public void contextInitialized(ServletContextEvent arg0) {
        LOG.info("RegistryContextListener has been loaded");

        try {
            RegistryBuilder.buildRegistry();
            this.serverRegistry = GDataServerRegistry.getRegistry();
            /*
             * catch all exceptions and destroy the registry to release all resources.
             * some components start lots of threads, the will remain running if the registry is not destroyed
             */
        } catch (Throwable e) {
            GDataServerRegistry.getRegistry().destroy();
            LOG.error("can not register required components", e);
            throw new RuntimeException("Can not register required components",
                    e);
        }
     

    }

    /**
     * @see javax.servlet.ServletContextListener#contextDestroyed(javax.servlet.ServletContextEvent)
     */
    public void contextDestroyed(ServletContextEvent arg0) {
        LOG.info("Destroying context");
        /*
         * this might be null if startup fails
         * --> prevent null pointer exception
         */
        if(this.serverRegistry != null)
            this.serverRegistry.destroy();

    }

}
