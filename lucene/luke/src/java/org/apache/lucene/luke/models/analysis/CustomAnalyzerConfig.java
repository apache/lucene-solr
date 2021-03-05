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

package org.apache.lucene.luke.models.analysis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Configurations for a custom analyzer. */
public final class CustomAnalyzerConfig {

  private final String configDir;

  private final ComponentConfig tokenizerConfig;

  private final List<ComponentConfig> charFilterConfigs;

  private final List<ComponentConfig> tokenFilterConfigs;

  /** Builder for {@link CustomAnalyzerConfig} */
  public static class Builder {
    private String configDir;
    private final ComponentConfig tokenizerConfig;
    private final List<ComponentConfig> charFilterConfigs = new ArrayList<>();
    private final List<ComponentConfig> tokenFilterConfigs = new ArrayList<>();

    public Builder(String name, Map<String, String> tokenizerParams) {
      Objects.requireNonNull(name);
      Objects.requireNonNull(tokenizerParams);
      tokenizerConfig = new ComponentConfig(name, new HashMap<>(tokenizerParams));
    }

    public Builder configDir(String val) {
      configDir = val;
      return this;
    }

    public Builder addCharFilterConfig(String name, Map<String, String> params) {
      Objects.requireNonNull(name);
      Objects.requireNonNull(params);
      charFilterConfigs.add(new ComponentConfig(name, new HashMap<>(params)));
      return this;
    }

    public Builder addTokenFilterConfig(String name, Map<String, String> params) {
      Objects.requireNonNull(name);
      Objects.requireNonNull(params);
      tokenFilterConfigs.add(new ComponentConfig(name, new HashMap<>(params)));
      return this;
    }

    public CustomAnalyzerConfig build() {
      return new CustomAnalyzerConfig(this);
    }
  }

  private CustomAnalyzerConfig(Builder builder) {
    this.tokenizerConfig = builder.tokenizerConfig;
    this.configDir = builder.configDir;
    this.charFilterConfigs = builder.charFilterConfigs;
    this.tokenFilterConfigs = builder.tokenFilterConfigs;
  }

  /** Returns directory path for configuration files, or empty. */
  Optional<String> getConfigDir() {
    return Optional.ofNullable(configDir);
  }

  /** Returns Tokenizer configurations. */
  ComponentConfig getTokenizerConfig() {
    return tokenizerConfig;
  }

  /** Returns CharFilters configurations. */
  List<ComponentConfig> getCharFilterConfigs() {
    return List.copyOf(charFilterConfigs);
  }

  /** Returns TokenFilters configurations. */
  List<ComponentConfig> getTokenFilterConfigs() {
    return List.copyOf(tokenFilterConfigs);
  }

  static class ComponentConfig {

    /* SPI name */
    private final String name;
    /* parameter map */
    private final Map<String, String> params;

    ComponentConfig(String name, Map<String, String> params) {
      this.name = Objects.requireNonNull(name);
      this.params = Objects.requireNonNull(params);
    }

    String getName() {
      return this.name;
    }

    Map<String, String> getParams() {
      return this.params;
    }
  }
}
