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

import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletRequestEvent;
import javax.servlet.ServletRequestListener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.gdata.server.registry.Scope.ScopeType;

/**
 * This <tt>ServletRequestListener</tt> is used by the registry to notify
 * registered {@link org.apache.lucene.gdata.server.registry.ScopeVisitor}
 * implementations when a request is initialized e.g destroyed.
 * 
 * 
 * @see org.apache.lucene.gdata.server.registry.ScopeVisitable
 * @see javax.servlet.ServletRequestListener
 * @author Simon Willnauer
 * 
 */
@Scope(scope = ScopeType.REQUEST)
public class GDataRequestListener implements ServletRequestListener,
        ScopeVisitable {
    private final GDataServerRegistry registry;

    private final List<ScopeVisitor> visitors = new ArrayList<ScopeVisitor>(5);

    private static final Log LOG = LogFactory
            .getLog(GDataRequestListener.class);

    /**
     * @throws RegistryException
     * 
     */
    public GDataRequestListener() throws RegistryException {
        this.registry = GDataServerRegistry.getRegistry();
        this.registry.registerScopeVisitable(this);

    }

    /**
     * @see javax.servlet.ServletRequestListener#requestDestroyed(javax.servlet.ServletRequestEvent)
     */
    public void requestDestroyed(ServletRequestEvent arg0) {
        for (ScopeVisitor visitor : this.visitors) {
            visitor.visiteDestroy();
        }

    }

    /**
     * @see javax.servlet.ServletRequestListener#requestInitialized(javax.servlet.ServletRequestEvent)
     */
    public void requestInitialized(ServletRequestEvent arg0) {
        for (ScopeVisitor visitor : this.visitors) {
            visitor.visiteInitialize();
        }
    }

    /**
     * @see org.apache.lucene.gdata.server.registry.ScopeVisitable#accept(org.apache.lucene.gdata.server.registry.ScopeVisitor)
     */
    public void accept(ScopeVisitor visitor) {

        if (!this.visitors.contains(visitor) && visitor != null) {
            this.visitors.add(visitor);
            if(LOG.isDebugEnabled())
                LOG.debug("visitor added -- " + visitor.getClass());
        }
    }

}
