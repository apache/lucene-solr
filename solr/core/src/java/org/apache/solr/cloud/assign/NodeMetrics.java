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

package org.apache.solr.cloud.assign;

import org.apache.solr.common.util.SimpleMap;

import java.util.function.Consumer;

public interface NodeMetrics {

    Property property();

    void keys(Consumer<String> keyConsumer);

    Object get(SimpleMap<Object> response);
    enum Property {
        SYSPROP("solr.jvm:system.properties:", "/metrics/solr.jvm/system.properties/tag" , String.class) {
            @Override
            public NodeMetrics getMetrics(final Object tag) {
                if(tag == null) throw new RuntimeException("system property name must not be null");
                return new NodeMetrics() {
                    @Override
                    public Property property() {
                        return SYSPROP;
                    }

                    @Override
                    public void keys(Consumer<String> keyConsumer) {
                        keyConsumer.accept(SYSPROP.metricsKey + tag);
                    }

                    @Override
                    public String get(SimpleMap<Object> response) {
                        return (String) response._get(SYSPROP.responsePath + tag, null);
                    }
                };
            }
        },
        CORES("solr.node:CONTAINER.cores", "/metrics/solr.node/CONTAINER.cores/loaded" , Long.class),
        FREEDISKSPACE("solr.node:CONTAINER.fs.usableSpace", "/metrics/solr.node/CONTAINER.fs.usableSpace",Long.class),
        TOTALDISKSPCE("solr.node:CONTAINER.fs.totalSpace", "/metrics/solr.node/CONTAINER.fs.totalSpace", Long.class),
        DISKTYPE("solr.node:CONTAINER.fs.coreRoot.spins",  "/metrics/solr.node/CONTAINER.fs.coreRoot.spins", String.class),

        ;

        private final String metricsKey;
        @SuppressWarnings("rawtypes")
        public final Class type;
        final String responsePath;

        @SuppressWarnings("rawtypes")
        Property(String key, String responsePath, Class typ) {
            this.metricsKey = key;
            this.type = typ;
            this.responsePath = responsePath;
        }

        public NodeMetrics getMetrics(){
            return getMetrics(null);
        }

        public NodeMetrics getMetrics(Object tag) {
            return new NodeMetrics() {
                @Override
                public Property property() {
                    return Property.this;
                }

                @Override
                public void keys(Consumer<String> keyConsumer) {
                    keyConsumer.accept(metricsKey);
                }

                @Override
                public Object get(SimpleMap<Object> response) {
                    return response._get(responsePath, null);
                }
            };

        }

    }


}
