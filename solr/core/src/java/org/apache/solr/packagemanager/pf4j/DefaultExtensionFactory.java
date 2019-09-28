/*
 * Copyright 2014 Decebal Suiu
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
package org.apache.solr.packagemanager.pf4j;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default implementation for ExtensionFactory.
 * It uses Class.newInstance() method.
 *
 * @author Decebal Suiu
 */
public class DefaultExtensionFactory implements ExtensionFactory {

    private static final Logger log = LoggerFactory.getLogger(DefaultExtensionFactory.class);

    /**
     * Creates an extension instance. If an error occurs than that error is logged and the method returns null.
     */
    @Override
    public Object create(Class<?> extensionClass) {
        log.debug("Create instance for extension '{}'", extensionClass.getName());
        try {
            return extensionClass.newInstance();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        return null;
    }

}
