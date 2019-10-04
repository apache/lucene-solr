/*
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
package org.apache.solr.store.blob.client;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Serializing/deserializing Json (mostly for {@link org.apache.solr.store.blob.client.BlobCoreMetadata}).
 */
public class ToFromJson<T> {
    /** Create easier to (human) read but a bit longer json output */
    static final boolean PRETTY_JSON = true;

    /**
     * Builds an object instance from a String representation of the Json.
     */
    public T fromJson(String input, Class<T> c) throws Exception {
        Gson gson = new Gson();
        return gson.fromJson(input, c);
    }

    /**
     * Returns the Json String of the passed object instance.
     */
    public String toJson(T t) throws Exception {
        Gson gson = PRETTY_JSON ? new GsonBuilder().setPrettyPrinting().create() : new Gson();
        return gson.toJson(t);
    }
}
