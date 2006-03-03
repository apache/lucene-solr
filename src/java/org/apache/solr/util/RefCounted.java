/**
 * Copyright 2006 The Apache Software Foundation
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

package org.apache.solr.util;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author yonik
 * @version $Id$
 */

public abstract class RefCounted<Type> {
  protected final Type resource;
  protected final AtomicInteger refcount= new AtomicInteger();
  public RefCounted(Type resource) { this.resource = resource; }
  public final RefCounted<Type> incref() { refcount.incrementAndGet(); return this; }
  public final Type get() { return resource; }
  public void decref() { if (refcount.decrementAndGet()==0) close(); }
  protected abstract void close();
}
