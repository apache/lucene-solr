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
package org.apache.lucene.gdata.server;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.gdata.server.administration.AdminService;
import org.apache.lucene.gdata.server.administration.GDataAdminService;
import org.apache.lucene.gdata.server.registry.Component;
import org.apache.lucene.gdata.server.registry.ComponentType;
import org.apache.lucene.gdata.server.registry.ServerComponent;


/**
 * The {@link ServiceFactory} creates {@link Service} implementations to access
 * the GData - Server components.
 * This class should not be access directy. The class will be registered in the {@link org.apache.lucene.gdata.server.registry.GDataServerRegistry}.
 * Use {@link org.apache.lucene.gdata.server.registry.GDataServerRegistry#lookup(Class, ComponentType)}
 * 
 * @author Simon Willnauer
 * 
 */
@Component(componentType=ComponentType.SERVICEFACTORY)
public class ServiceFactory implements ServerComponent {

    private static final Log LOG = LogFactory.getLog(ServiceFactory.class);

	

	/**
	 * public constructor to enable loading via the registry
     * @see org.apache.lucene.gdata.server.registry.Component
     * @see org.apache.lucene.gdata.server.registry.GDataServerRegistry
	 */
	public ServiceFactory() {
		//
	}

	/**
	 * Creates a {@link Service} instance.
	 * 
	 * @return a Service instance
	 */
	public Service getService() {
		try{
		return new GDataService();
        }catch (Exception e) {
            //
        }
        return null;
	}
    
    /**
     * Creates a {@link AdminService} instance
     * @return a AdminService instance
     */
    public AdminService getAdminService(){
        try {
            return new GDataAdminService();
        } catch (ServiceException e) {
           LOG.warn("Factory method can not create GDataAdminService returning null-- "+e.getMessage(),e);
        }
        return null;
    }

    /**
     * @see org.apache.lucene.gdata.server.registry.ServerComponent#initialize()
     */
    public void initialize() {
        //
    }

    /**
     * @see org.apache.lucene.gdata.server.registry.ServerComponent#destroy()
     */
    public void destroy() {
        //
    }
}
