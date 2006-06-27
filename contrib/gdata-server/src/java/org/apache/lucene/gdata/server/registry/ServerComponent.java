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

/**
 * To Register a class as a component in the
 * {@link org.apache.lucene.gdata.server.registry.GDataServerRegistry} the class
 * or a super class must implements this interface.
 * <p>
 * <tt>ServerComponent</tt> defines a method <tt>initialize</tt> and
 * <tt>destroy</tt>. <tt>initialize</tt> will be called when the component
 * is registered and <tt>destroy</tt> when the registry is destroyed (usually
 * at server shut down).</p>
 * @see org.apache.lucene.gdata.server.registry.GDataServerRegistry
 * @author Simon Willnauer
 * 
 */
public interface ServerComponent {
    /**
     * will be call when the component is registered.
     */
    public abstract void initialize();

    /**
     * will be called when the registry is going down e.g. when the  {@link GDataServerRegistry#destroy()} method is called.
     */
    public abstract void destroy();
}
