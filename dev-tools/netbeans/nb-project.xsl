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
                xmlns:common="http://exslt.org/common"
                extension-element-prefixes="str common">
  <xsl:param name="netbeans.fileset.sourcefolders"/>
  <xsl:param name="netbeans.path.libs"/>
  <xsl:param name="netbeans.source-level"/>
  
  <xsl:variable name="netbeans.fileset.sourcefolders.sortedfrag">
    <xsl:for-each select="str:split($netbeans.fileset.sourcefolders,'|')">
      <!-- hack to sort **/src/java before **/src/test before **/src/resources : contains() returns "true" which sorts before "false" if descending: -->
      <xsl:sort select="string(contains(text(), '/src/java'))" order="descending" lang="en"/>
      <xsl:sort select="string(contains(text(), '/src/test'))" order="descending" lang="en"/>
      <xsl:sort select="string(contains(text(), '/src/resources'))" order="descending" lang="en"/>
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
      <xsl:copy-of select="."/>
    </xsl:for-each>
  </xsl:variable>
  <xsl:variable name="netbeans.fileset.sourcefolders.sorted" select="common:node-set($netbeans.fileset.sourcefolders.sortedfrag)/*"/>
  
  <xsl:variable name="netbeans.full.classpath.frag">
    <classpath mode="compile" xmlns="http://www.netbeans.org/ns/freeform-project-java/3">
      <xsl:value-of select="$netbeans.path.libs"/>
      <xsl:for-each select="$netbeans.fileset.sourcefolders.sorted[contains(text(), '/src/java')]">
        <xsl:text>:</xsl:text>
        <xsl:value-of select="."/>
      </xsl:for-each>
    </classpath>
  </xsl:variable>

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
            <xsl:for-each select="$netbeans.fileset.sourcefolders.sorted">
              <source-folder>
                <label>
                  <xsl:value-of select="."/>
                </label>
                <xsl:if test="contains(text(), '/src/java') or contains(text(), '/src/test')">
                  <type>java</type>
                </xsl:if>
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
              <xsl:for-each select="$netbeans.fileset.sourcefolders.sorted">
                <source-folder>
                  <xsl:attribute name="style">
                    <xsl:choose>
                      <xsl:when test="contains(text(), '/src/java') or contains(text(), '/src/test')">packages</xsl:when>
                      <xsl:otherwise>tree</xsl:otherwise>
                    </xsl:choose>
                  </xsl:attribute>
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
            <xsl:for-each select="$netbeans.fileset.sourcefolders.sorted[contains(text(), '/src/java')]">
              <package-root>
                <xsl:value-of select="."/>
              </package-root>
            </xsl:for-each>
            <xsl:copy-of select="$netbeans.full.classpath.frag"/>
            <built-to>nb-build/classes</built-to>
            <source-level>
              <xsl:value-of select="$netbeans.source-level"/>
            </source-level>
          </compilation-unit>
          <compilation-unit>
            <xsl:for-each select="$netbeans.fileset.sourcefolders.sorted[contains(text(), '/src/test')]">
              <package-root>
                <xsl:value-of select="."/>
              </package-root>
            </xsl:for-each>
            <unit-tests/>
            <xsl:copy-of select="$netbeans.full.classpath.frag"/>
            <built-to>nb-build/test-classes</built-to>
            <source-level>
              <xsl:value-of select="$netbeans.source-level"/>
            </source-level>
          </compilation-unit>
        </java-data>
      </configuration>
    </project>
  </xsl:template>
</xsl:stylesheet>
