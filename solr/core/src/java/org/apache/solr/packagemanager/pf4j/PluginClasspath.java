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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The classpath of the plugin.
 * It contains {@code classes} directories and {@code lib} directories (directories that contains jars).
 *
 * @author Decebal Suiu
 */
public class PluginClasspath {

	private List<String> classesDirectories;
	private List<String> libDirectories;

	public PluginClasspath() {
		classesDirectories = new ArrayList<>();
		libDirectories = new ArrayList<>();
	}

	public List<String> getClassesDirectories() {
		return classesDirectories;
	}

	public void addClassesDirectories(String... classesDirectories) {
		this.classesDirectories.addAll(Arrays.asList(classesDirectories));
	}

	public List<String> getLibDirectories() {
		return libDirectories;
	}

	public void addLibDirectories(String... libDirectories) {
		this.libDirectories.addAll(Arrays.asList(libDirectories));
	}

}
