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

package org.apache.solr.common;

import org.apache.solr.cluster.api.Resource;
import org.apache.solr.cluster.api.SimpleMap;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.zookeeper.KeeperException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**A view of ZK as a {@link SimpleMap} impl. This gives a flat view of all paths instead of a tree view
 * eg: a, b, c , a/a1, a/a2, a/a1/aa1 etc
 * If possible,  use the {@link #abortableForEach(BiFunction)} to traverse
 * DO not use the {@link #size()} method. It always return 0 because it is very expensive to compute that
 *
 */
public class SimpleZkMap implements SimpleMap<Resource> {
    private final ZkStateReader zkStateReader;
    private final String basePath;

    static final byte[] EMPTY_BYTES = new byte[0];


    public SimpleZkMap(ZkStateReader zkStateReader, String path) {
        this.zkStateReader = zkStateReader;
        this.basePath = path;
    }


    @Override
    public Resource get(String key) {
        return readZkNode(basePath + key);
    }

    @Override
    public void abortableForEach(BiFunction<String, ? super Resource, Boolean> fun) {
        try {
            recursiveRead("",
                    zkStateReader.getZkClient().getChildren(basePath, null, true),
                    fun);
        } catch (KeeperException | InterruptedException e) {
            throwZkExp(e);
        }
    }

    @Override
    public void forEachEntry(BiConsumer<String, ? super Resource> fun) {
        abortableForEach((path, resource) -> {
            fun.accept(path, resource);
            return Boolean.TRUE;
        });
    }

    @Override
    public int size() {
        return 0;
    }

    private Resource readZkNode(String path) {
        return new Resource() {
            @Override
            public String name() {
                return path;
            }

            @Override
            public void get(Consumer consumer) throws SolrException {
                try {
                    byte[] data = zkStateReader.getZkClient().getData(basePath+"/"+  path, null, null, true);
                    if (data != null && data.length > 0) {
                        consumer.read(new ByteArrayInputStream(data));
                    } else {
                        consumer.read(new ByteArrayInputStream(EMPTY_BYTES));
                    }
                } catch (KeeperException | InterruptedException e) {
                    throwZkExp(e);
                } catch (IOException e) {
                    throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Can;t read stream" , e);
                }

            }
        };
    }

    private boolean recursiveRead(String parent, List<String> childrenList, BiFunction<String, ? super Resource, Boolean> fun) {
        if(childrenList == null || childrenList.isEmpty()) return true;
        try {
            Map<String, List<String>> withKids = new LinkedHashMap<>();
            for (String child : childrenList) {
                String relativePath =  parent.isEmpty() ? child: parent+"/"+child;
                if(!fun.apply(relativePath, readZkNode(relativePath))) return false;
                List<String> l1 =  zkStateReader.getZkClient().getChildren(basePath+ "/"+ relativePath, null, true);
                if(l1 != null && !l1.isEmpty()) {
                    withKids.put(relativePath, l1);
                }
            }
            //now we iterate through all nodes with sub paths
            for (Map.Entry<String, List<String>> e : withKids.entrySet()) {
                //has children
                if(!recursiveRead(e.getKey(), e.getValue(), fun)) {
                    return false;
                }
            }
        } catch (KeeperException | InterruptedException e) {
            throwZkExp(e);
        }
        return true;
    }

    static void throwZkExp(Exception e) {
        if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "ZK errror", e);
    }

}
