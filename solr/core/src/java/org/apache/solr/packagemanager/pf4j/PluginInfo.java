/*
 * Copyright (C) 2012-present the original author or authors.
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

import java.io.Serializable;
import java.util.Date;
import java.util.List;

/**
 * {@code PluginInfo} describing a plugin from a repository.
 */
public class PluginInfo implements Serializable, Comparable<PluginInfo> {

    public String id;
    public String name;
    public String description;
    public String provider;
    public String projectUrl;
    public List<PluginRelease> releases;

    // This is metadata added at parse time, not part of the published plugins.json
    private String repositoryId;


    @Override
    public int compareTo(PluginInfo o) {
        return id.compareTo(o.id);
    }

    public String getRepositoryId() {
        return repositoryId;
    }

    public void setRepositoryId(String repositoryId) {
        this.repositoryId = repositoryId;
    }

    @Override
    public String toString() {
        return "PluginInfo{" +
            "id='" + id + '\'' +
            ", name='" + name + '\'' +
            ", description='" + description + '\'' +
            ", provider='" + provider + '\'' +
            ", projectUrl='" + projectUrl + '\'' +
            ", releases=" + releases +
            ", repositoryId='" + repositoryId + '\'' +
            '}';
    }

    /**
     * A concrete release.
     */
    public static class PluginRelease implements Serializable {

        public String version;
        public Date date;
        public String requires;
        public String url;
        /**
         * Optional sha512 digest checksum. Can be one of
         * <ul>
         *   <li>&lt;sha512 sum string&gt;</li>
         *   <li>URL to an external sha512 file</li>
         *   <li>".sha512" as a shortcut for saying download a &lt;filename&gt;.sha512 file next to the zip/jar file</li>
         * </ul>
         */
        public String sha512sum;

        @Override
        public String toString() {
            return "PluginRelease{" +
                "version='" + version + '\'' +
                ", date=" + date +
                ", requires='" + requires + '\'' +
                ", url='" + url + '\'' +
                ", sha512sum='" + sha512sum + '\'' +
                '}';
        }
    }

}
