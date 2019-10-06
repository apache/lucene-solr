/*
 * Copyright 2017 Decebal Suiu
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

import com.github.zafarkhaja.semver.Version;
import com.github.zafarkhaja.semver.expr.Expression;

/**
 * This implementation uses jSemVer (a Java implementation of the SemVer Specification).
 *
 * @author Decebal Suiu
 */
public class DefaultVersionManager {

    /**
     * Checks if a version satisfies the specified SemVer {@link Expression} string.
     * If the constraint is empty or null then the method returns true.
     * Constraint examples: {@code >2.0.0} (simple), {@code ">=1.4.0 & <1.6.0"} (range).
     * See https://github.com/zafarkhaja/jsemver#semver-expressions-api-ranges for more info.
     *
     */
    public boolean checkVersionConstraint(String version, String constraint) {
        return StringUtils.isNullOrEmpty(constraint) || Version.valueOf(version).satisfies(constraint);
    }

    public int compareVersions(String v1, String v2) {
        return Version.valueOf(v1).compareTo(Version.valueOf(v2));
    }

}
