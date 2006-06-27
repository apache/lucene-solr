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

import org.apache.lucene.gdata.server.registry.Component;
import org.apache.lucene.gdata.server.registry.ComponentType;
import org.apache.lucene.gdata.storage.Storage;
import org.apache.lucene.gdata.storage.StorageController;
import org.apache.lucene.gdata.storage.StorageException;

/**
 * @author Simon Willnauer
 *
 */
@Component(componentType = ComponentType.STORAGECONTROLLER)
public class StorageControllerStub implements StorageController {

    /**
     * 
     */
    public StorageControllerStub() {
        super();
        // TODO Auto-generated constructor stub
    }

    /**
     * @see org.apache.lucene.gdata.storage.StorageController#destroy()
     */
    public void destroy() {
    }

    /**
     * @see org.apache.lucene.gdata.storage.StorageController#getStorage()
     */
    public Storage getStorage() throws StorageException {

        return new StorageStub();
    }

    /**
     * @see org.apache.lucene.gdata.server.registry.ServerComponent#initialize()
     */
    public void initialize() {
    }

}
