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

import org.pf4j.util.StringUtils;

/**
 * An exception used to indicate that a plugin problem occurred.
 *
 * @author Decebal Suiu
 */
public class PluginException extends Exception {

	public PluginException() {
        super();
    }

    public PluginException(String message) {
        super(message);
    }

    public PluginException(Throwable cause) {
        super(cause);
    }

    public PluginException(String message, Throwable cause) {
        super(message, cause);
    }

    public PluginException(Throwable cause, String message, Object... args) {
        super(StringUtils.format(message, args), cause);
    }

    public PluginException(String message, Object... args) {
        super(StringUtils.format(message, args));
    }

}
