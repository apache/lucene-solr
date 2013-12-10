<?xml version="1.0" encoding="UTF-8"?>
<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
<xsl:stylesheet version="1.0" 
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:str="http://exslt.org/strings"
                extension-element-prefixes="str">
    <xsl:param name="netbeans.fileset.sourcefolders"/>
    <xsl:param name="netbeans.fileset.testfolders"/>
    <xsl:param name="netbeans.fileset.resourcefolders"/>
    <xsl:param name="netbeans.fileset.libs"/>
  

    <!--
      NOTE: This template matches the root element of any given input XML document!
      The XSL input file is ignored completely.
    --> 
    <xsl:template match="/">
        <project xmlns="http://www.netbeans.org/ns/project/1">
            <type>org.netbeans.modules.ant.freeform</type>
            <configuration>
                <general-data xmlns="http://www.netbeans.org/ns/freeform-project/1">
                    <name>lucene</name>
                    <properties/>
                    <folders>
                        <xsl:for-each select="str:split($netbeans.fileset.sourcefolders,'|')">
                            <!-- hack to sort the list, starts-with() returns "true" which sorts before "false" if descending: -->
                            <xsl:sort select="string(starts-with(text(), 'lucene/core/'))" order="descending" lang="en"/>
                            <xsl:sort select="string(starts-with(text(), 'lucene/test-framework/'))" order="descending" lang="en"/>
                            <xsl:sort select="string(starts-with(text(), 'lucene/'))" order="descending" lang="en"/>
                            <xsl:sort select="string(starts-with(text(), 'solr/core/'))" order="descending" lang="en"/>
                            <xsl:sort select="string(starts-with(text(), 'solr/solrj/'))" order="descending" lang="en"/>
                            <xsl:sort select="string(starts-with(text(), 'solr/test-framework/'))" order="descending" lang="en"/>
                            <xsl:sort select="string(starts-with(text(), 'solr/'))" order="descending" lang="en"/>
                            <!-- all others in one group above are sorted by path name: -->
                            <xsl:sort select="text()" order="ascending" lang="en"/>
                            <source-folder>
                                <label>
                                    <xsl:value-of select="."/>
                                </label>
                                <type>java</type>
                                <location>
                                    <xsl:value-of select="."/>
                                </location>
                            </source-folder>
                        </xsl:for-each>
                        <xsl:for-each select="str:split($netbeans.fileset.testfolders,'|')">
                            <!-- hack to sort the list, starts-with() returns "true" which sorts before "false" if descending: -->
                            <xsl:sort select="string(starts-with(text(), 'lucene/core/'))" order="descending" lang="en"/>
                            <xsl:sort select="string(starts-with(text(), 'lucene/test-framework/'))" order="descending" lang="en"/>
                            <xsl:sort select="string(starts-with(text(), 'lucene/'))" order="descending" lang="en"/>
                            <xsl:sort select="string(starts-with(text(), 'solr/core/'))" order="descending" lang="en"/>
                            <xsl:sort select="string(starts-with(text(), 'solr/solrj/'))" order="descending" lang="en"/>
                            <xsl:sort select="string(starts-with(text(), 'solr/test-framework/'))" order="descending" lang="en"/>
                            <xsl:sort select="string(starts-with(text(), 'solr/'))" order="descending" lang="en"/>
                            <!-- all others in one group above are sorted by path name: -->
                            <xsl:sort select="text()" order="ascending" lang="en"/>
                            <source-folder>
                                <label>
                                    <xsl:value-of select="."/>
                                </label>
                                <type>java</type>
                                <location>
                                    <xsl:value-of select="."/>
                                </location>
                            </source-folder>
                        </xsl:for-each>
                        <xsl:for-each select="str:split($netbeans.fileset.resourcefolders,'|')">
                            <!-- hack to sort the list, starts-with() returns "true" which sorts before "false" if descending: -->
                            <xsl:sort select="string(starts-with(text(), 'lucene/core/'))" order="descending" lang="en"/>
                            <xsl:sort select="string(starts-with(text(), 'lucene/test-framework/'))" order="descending" lang="en"/>
                            <xsl:sort select="string(starts-with(text(), 'lucene/'))" order="descending" lang="en"/>
                            <xsl:sort select="string(starts-with(text(), 'solr/core/'))" order="descending" lang="en"/>
                            <xsl:sort select="string(starts-with(text(), 'solr/solrj/'))" order="descending" lang="en"/>
                            <xsl:sort select="string(starts-with(text(), 'solr/test-framework/'))" order="descending" lang="en"/>
                            <xsl:sort select="string(starts-with(text(), 'solr/'))" order="descending" lang="en"/>
                            <!-- all others in one group above are sorted by path name: -->
                            <xsl:sort select="text()" order="ascending" lang="en"/>
                            <source-folder>
                                <label>
                                    <xsl:value-of select="."/>
                                </label>
                                <location>
                                    <xsl:value-of select="."/>
                                </location>
                            </source-folder>
                        </xsl:for-each>
                    </folders>
                    <ide-actions>
                        <action name="build">
                            <target>compile</target>
                        </action>
                        <action name="clean">
                            <target>clean</target>
                        </action>
                        <action name="javadoc">
                            <target>documentation</target>
                        </action>
                        <action name="test">
                            <target>test</target>
                        </action>
                        <action name="rebuild">
                            <target>clean</target>
                            <target>compile</target>
                        </action>
                    </ide-actions>
                    <view>
                        <items>
                            <xsl:for-each select="str:split($netbeans.fileset.sourcefolders,'|')">
                                <!-- hack to sort the list, starts-with() returns "true" which sorts before "false" if descending: -->
                                <xsl:sort select="string(starts-with(text(), 'lucene/core/'))" order="descending" lang="en"/>
                                <xsl:sort select="string(starts-with(text(), 'lucene/test-framework/'))" order="descending" lang="en"/>
                                <xsl:sort select="string(starts-with(text(), 'lucene/'))" order="descending" lang="en"/>
                                <xsl:sort select="string(starts-with(text(), 'solr/core/'))" order="descending" lang="en"/>
                                <xsl:sort select="string(starts-with(text(), 'solr/solrj/'))" order="descending" lang="en"/>
                                <xsl:sort select="string(starts-with(text(), 'solr/test-framework/'))" order="descending" lang="en"/>
                                <xsl:sort select="string(starts-with(text(), 'solr/'))" order="descending" lang="en"/>
                                <!-- all others in one group above are sorted by path name: -->
                                <xsl:sort select="text()" order="ascending" lang="en"/>
                                <source-folder style="packages">
                                    <label>
                                        <xsl:value-of select="."/>
                                    </label>
                                    <location>
                                        <xsl:value-of select="."/>
                                    </location>
                                </source-folder>
                            </xsl:for-each>
                            <xsl:for-each select="str:split($netbeans.fileset.testfolders,'|')">
                                <!-- hack to sort the list, starts-with() returns "true" which sorts before "false" if descending: -->
                                <xsl:sort select="string(starts-with(text(), 'lucene/core/'))" order="descending" lang="en"/>
                                <xsl:sort select="string(starts-with(text(), 'lucene/test-framework/'))" order="descending" lang="en"/>
                                <xsl:sort select="string(starts-with(text(), 'lucene/'))" order="descending" lang="en"/>
                                <xsl:sort select="string(starts-with(text(), 'solr/core/'))" order="descending" lang="en"/>
                                <xsl:sort select="string(starts-with(text(), 'solr/solrj/'))" order="descending" lang="en"/>
                                <xsl:sort select="string(starts-with(text(), 'solr/test-framework/'))" order="descending" lang="en"/>
                                <xsl:sort select="string(starts-with(text(), 'solr/'))" order="descending" lang="en"/>
                                <!-- all others in one group above are sorted by path name: -->
                                <xsl:sort select="text()" order="ascending" lang="en"/>
                                <source-folder style="packages">
                                    <label>
                                        <xsl:value-of select="."/>
                                    </label>
                                    <location>
                                        <xsl:value-of select="."/>
                                    </location>
                                </source-folder>
                            </xsl:for-each>
                            <xsl:for-each select="str:split($netbeans.fileset.resourcefolders,'|')">
                                <!-- hack to sort the list, starts-with() returns "true" which sorts before "false" if descending: -->
                                <xsl:sort select="string(starts-with(text(), 'lucene/core/'))" order="descending" lang="en"/>
                                <xsl:sort select="string(starts-with(text(), 'lucene/test-framework/'))" order="descending" lang="en"/>
                                <xsl:sort select="string(starts-with(text(), 'lucene/'))" order="descending" lang="en"/>
                                <xsl:sort select="string(starts-with(text(), 'solr/core/'))" order="descending" lang="en"/>
                                <xsl:sort select="string(starts-with(text(), 'solr/solrj/'))" order="descending" lang="en"/>
                                <xsl:sort select="string(starts-with(text(), 'solr/test-framework/'))" order="descending" lang="en"/>
                                <xsl:sort select="string(starts-with(text(), 'solr/'))" order="descending" lang="en"/>
                                <!-- all others in one group above are sorted by path name: -->
                                <xsl:sort select="text()" order="ascending" lang="en"/>
                                <source-folder style="tree">
                                    <label>
                                        <xsl:value-of select="."/>
                                    </label>
                                    <location>
                                        <xsl:value-of select="."/>
                                    </location>
                                </source-folder>
                            </xsl:for-each>
                            <source-file>
                                <label>Project Build Script</label>
                                <location>build.xml</location>
                            </source-file>
                        </items>
                        <context-menu>
                            <ide-action name="build"/>
                            <ide-action name="rebuild"/>
                            <ide-action name="clean"/>
                            <ide-action name="javadoc"/>
                            <ide-action name="test"/>
                        </context-menu>
                    </view>
                    <subprojects/>
                </general-data>
                <java-data xmlns="http://www.netbeans.org/ns/freeform-project-java/3">
                    <compilation-unit>
                        <xsl:for-each select="str:split($netbeans.fileset.sourcefolders,'|')">
                            <!-- hack to sort the list, starts-with() returns "true" which sorts before "false" if descending: -->
                            <xsl:sort select="string(starts-with(text(), 'lucene/core/'))" order="descending" lang="en"/>
                            <xsl:sort select="string(starts-with(text(), 'lucene/test-framework/'))" order="descending" lang="en"/>
                            <xsl:sort select="string(starts-with(text(), 'lucene/'))" order="descending" lang="en"/>
                            <xsl:sort select="string(starts-with(text(), 'solr/core/'))" order="descending" lang="en"/>
                            <xsl:sort select="string(starts-with(text(), 'solr/solrj/'))" order="descending" lang="en"/>
                            <xsl:sort select="string(starts-with(text(), 'solr/test-framework/'))" order="descending" lang="en"/>
                            <xsl:sort select="string(starts-with(text(), 'solr/'))" order="descending" lang="en"/>
                            <!-- all others in one group above are sorted by path name: -->
                            <xsl:sort select="text()" order="ascending" lang="en"/>
                            <package-root>
                                <xsl:value-of select="."/>
                            </package-root>
                        </xsl:for-each>
                        <classpath mode="compile">
                            <xsl:for-each select="str:split($netbeans.fileset.libs,'|')">
                                <!-- sort the jars by path name: -->
                                <xsl:sort select="text()" order="ascending" lang="en"/>
                                <xsl:value-of select="."/>
                                <xsl:if test="not(position() = last())">
                                    <xsl:text>:</xsl:text>
                                </xsl:if>
                            </xsl:for-each>
                            <xsl:text>:</xsl:text>
                            <xsl:for-each select="str:split($netbeans.fileset.sourcefolders,'|')">
                                <!-- sort the jars by path name: -->
                                <xsl:sort select="text()" order="ascending" lang="en"/>
                                <xsl:value-of select="."/>
                                <xsl:if test="not(position() = last())">
                                    <xsl:text>:</xsl:text>
                                </xsl:if>
                            </xsl:for-each>
                        </classpath>
                        <built-to>nb-build/classes</built-to>
                        <source-level>1.7</source-level>
                    </compilation-unit>
                    <compilation-unit>
                        <xsl:for-each select="str:split($netbeans.fileset.testfolders,'|')">
                            <!-- hack to sort the list, starts-with() returns "true" which sorts before "false" if descending: -->
                            <xsl:sort select="string(starts-with(text(), 'lucene/core/'))" order="descending" lang="en"/>
                            <xsl:sort select="string(starts-with(text(), 'lucene/test-framework/'))" order="descending" lang="en"/>
                            <xsl:sort select="string(starts-with(text(), 'lucene/'))" order="descending" lang="en"/>
                            <xsl:sort select="string(starts-with(text(), 'solr/core/'))" order="descending" lang="en"/>
                            <xsl:sort select="string(starts-with(text(), 'solr/solrj/'))" order="descending" lang="en"/>
                            <xsl:sort select="string(starts-with(text(), 'solr/test-framework/'))" order="descending" lang="en"/>
                            <xsl:sort select="string(starts-with(text(), 'solr/'))" order="descending" lang="en"/>
                            <!-- all others in one group above are sorted by path name: -->
                            <xsl:sort select="text()" order="ascending" lang="en"/>
                            <package-root>
                                <xsl:value-of select="."/>
                            </package-root>
                        </xsl:for-each>
                        <unit-tests/>
                        <classpath mode="compile">
                            <xsl:for-each select="str:split($netbeans.fileset.libs,'|')">
                                <!-- sort the jars by path name: -->
                                <xsl:sort select="text()" order="ascending" lang="en"/>
                                <xsl:value-of select="."/>
                                <xsl:if test="not(position() = last())">
                                    <xsl:text>:</xsl:text>
                                </xsl:if>
                            </xsl:for-each>
                            <xsl:text>:</xsl:text>
                            <xsl:for-each select="str:split($netbeans.fileset.sourcefolders,'|')">
                                <!-- sort the jars by path name: -->
                                <xsl:sort select="text()" order="ascending" lang="en"/>
                                <xsl:value-of select="."/>
                                <xsl:if test="not(position() = last())">
                                    <xsl:text>:</xsl:text>
                                </xsl:if>
                            </xsl:for-each>
                        </classpath>
                        <built-to>nb-build/test-classes</built-to>
                        <source-level>1.7</source-level>
                    </compilation-unit>
                </java-data>
            </configuration>
        </project>
    </xsl:template>
</xsl:stylesheet>
