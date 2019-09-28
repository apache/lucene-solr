/*
 * Copyright 2013 Decebal Suiu
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

/**
 * Overwrite classes directories to {@code target/classes} and lib directories to {@code target/lib}.
 *
 * @author Decebal Suiu
 */
public class DevelopmentPluginClasspath extends PluginClasspath {

	public DevelopmentPluginClasspath() {
		super();

        addClassesDirectories("target/classes");
        addLibDirectories("target/lib");
    }

}
