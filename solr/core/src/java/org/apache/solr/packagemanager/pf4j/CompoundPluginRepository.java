/*
 * Copyright 2012 Decebal Suiu
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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Decebal Suiu
 * @author Mário Franco
 */
public class CompoundPluginRepository implements PluginRepository {

    private List<PluginRepository> repositories = new ArrayList<>();

    public CompoundPluginRepository add(PluginRepository repository) {
        if (repository == null) {
            throw new IllegalArgumentException("null not allowed");
        }

        repositories.add(repository);

        return this;
    }

    @Override
    public List<Path> getPluginPaths() {
        List<Path> paths = new ArrayList<>();
        for (PluginRepository repository : repositories) {
            paths.addAll(repository.getPluginPaths());
        }

        return paths;
    }

    @Override
    public boolean deletePluginPath(Path pluginPath) {
        for (PluginRepository repository : repositories) {
            if (repository.deletePluginPath(pluginPath)) {
                return true;
            }
        }

        return false;
    }

}
