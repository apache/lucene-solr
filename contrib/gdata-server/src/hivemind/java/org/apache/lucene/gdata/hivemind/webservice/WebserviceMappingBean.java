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

package org.apache.lucene.gdata.hivemind.webservice;

/**
 * This class is a simple configuration bean to expose a certain service via a
 * hessian webservice. The configuration requieres the classtype of the
 * interface and an instance of a subclass to invoke the interface methodes.
 * <p>
 * This bean will be created by Hivemind for each configured service and will be
 * passed to the
 * {@link org.apache.lucene.gdata.hivemind.webservice.HessianSkeletonProvider}
 * as a Map.
 * </p>
 * 
 * @author Simon Willnauer
 * 
 */
public class WebserviceMappingBean {
    private Class serviceInterface;

    private Object serviceImpl;

    /**
     * Bean constructor
     */
    public WebserviceMappingBean() {
        super();

    }

    /**
     * @return Returns the serviceImpl.
     */
    public Object getServiceImpl() {
        return this.serviceImpl;
    }

    /**
     * @param serviceImpl
     *            The serviceImpl to set.
     */
    public void setServiceImpl(Object serviceImpl) {
        this.serviceImpl = serviceImpl;
    }

    /**
     * @return Returns the serviceInterface.
     */
    public Class getServiceInterface() {
        return this.serviceInterface;
    }

    /**
     * @param serviceInterface
     *            The serviceInterface to set.
     */
    public void setServiceInterface(Class serviceInterface) {
        this.serviceInterface = serviceInterface;
    }

}
