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

import org.pf4j.util.FileUtils;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Decebal Suiu
 * @author Mário Franco
 */
public class BasePluginRepository implements PluginRepository {

    protected final Path pluginsRoot;
    protected FileFilter filter;

    public BasePluginRepository(Path pluginsRoot) {
        this.pluginsRoot = pluginsRoot;
    }

    public BasePluginRepository(Path pluginsRoot, FileFilter filter) {
        this.pluginsRoot = pluginsRoot;
        this.filter = filter;
    }

    public void setFilter(FileFilter filter) {
        this.filter = filter;
    }

    @Override
    public List<Path> getPluginPaths() {
        File[] files = pluginsRoot.toFile().listFiles(filter);

        if ((files == null) || files.length == 0) {
            return Collections.emptyList();
        }

        List<Path> paths = new ArrayList<>(files.length);
        for (File file : files) {
            paths.add(file.toPath());
        }

        return paths;
    }

    @Override
    public boolean deletePluginPath(Path pluginPath) {
        try {
            FileUtils.delete(pluginPath);
            return true;
        } catch (NoSuchFileException nsf) {
            return false; // Return false on not found to be compatible with previous API
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
