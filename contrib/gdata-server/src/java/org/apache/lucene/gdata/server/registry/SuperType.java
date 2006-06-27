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

package org.apache.lucene.gdata.server.registry;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * This Annotation is use to annotate
 * {@link org.apache.lucene.gdata.server.registry.ComponentType} elements to
 * specify an interface e.g. super type of a defined component.
 * <p>This annotation will be visible at runtime</p>
 * @see org.apache.lucene.gdata.server.registry.Component
 * @see org.apache.lucene.gdata.server.registry.GDataServerRegistry
 * 
 * @author Simon Willnauer
 * 
 */
@Target( { FIELD })
@Retention(value = RUNTIME)
public @interface SuperType {
    /**
     * 
     * @return the specified super type
     */
    Class superType();
}
