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

package org.apache.lucene.gdata.utils;

import org.apache.lucene.gdata.server.Service;
import org.apache.lucene.gdata.server.ServiceFactory;
import org.apache.lucene.gdata.server.administration.AdminService;
import org.apache.lucene.gdata.server.registry.Component;
import org.apache.lucene.gdata.server.registry.ComponentType;

/**
 * @author Simon Willnauer
 *
 */
@Component(componentType=ComponentType.SERVICEFACTORY)
public class ServiceFactoryStub extends ServiceFactory {
   
    public Service service;
    public AdminService adminService;
    /**
     * @see org.apache.lucene.gdata.server.ServiceFactory#getAdminService()
     */
    @Override
    public AdminService getAdminService() {
        
        return adminService;
    }

    /**
     * @see org.apache.lucene.gdata.server.ServiceFactory#getService()
     */
    @Override
    public Service getService() {
        
        return service;
    }

    public void setAdminService(AdminService service) {
        this.adminService = service;
    }
    public void setService(Service service) {
        this.service = service;
    }
    

}
