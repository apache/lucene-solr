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
package org.apache.solr.common.util;


import java.util.List;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;

import static org.apache.solr.common.util.Utils.toJSONString;
import static org.apache.solr.common.util.ValidatingJsonMap.NOT_NULL;

public class JsonValidatorTest extends SolrTestCaseJ4  {

  public void testSchema() {
    checkSchema("collections.Commands");
    checkSchema("collections.collection.Commands");
    checkSchema("collections.collection.shards.Commands");
    checkSchema("collections.collection.shards.shard.Commands");
    checkSchema("cores.Commands");
    checkSchema("cores.core.Commands");
    checkSchema("node.Commands");
    checkSchema("cluster.security.BasicAuth.Commands");
    checkSchema("cluster.security.RuleBasedAuthorization");
    checkSchema("core.config.Commands");
    checkSchema("core.SchemaEdit");
    checkSchema("cluster.configs.Commands");
    checkSchema("cluster.Commands");
  }


  public void testSchemaValidation() {
    ValidatingJsonMap spec = Utils.getSpec("collections.Commands").getSpec();
    Map createSchema = spec.getMap("commands", NOT_NULL).getMap("create-alias", NOT_NULL);
    JsonSchemaValidator validator = new JsonSchemaValidator(createSchema);
    List<String> errs = validator.validateJson(Utils.fromJSONString("{name : x, collections: [ c1 , c2]}"));
    assertNull(toJSONString(errs), errs);
    errs = validator.validateJson(Utils.fromJSONString("{name : x, collections: [c1] }"));
    assertNull(toJSONString(errs), errs);
    errs = validator.validateJson(Utils.fromJSONString("{name : x, x:y, collections: [ c1 , c2]}"));
    assertNotNull(toJSONString(errs), errs);
    assertTrue(toJSONString(errs), errs.get(0).contains("Unknown"));
    errs = validator.validateJson(Utils.fromJSONString("{name : 123, collections: c1 }"));
    assertNotNull(toJSONString(errs), errs);
    assertTrue(toJSONString(errs), errs.get(0).contains("expected"));
    errs = validator.validateJson(Utils.fromJSONString("{x:y, collections: [ c1 , c2]}"));
    assertTrue(toJSONString(errs), StrUtils.join(errs, '|').contains("Unknown"));
    errs = validator.validateJson(Utils.fromJSONString("{name : x, collections: [ 1 , 2]}"));
    assertFalse(toJSONString(errs), errs.isEmpty());
    assertTrue(toJSONString(errs), errs.get(0).contains("expected"));
    validator = new JsonSchemaValidator("{" +
        "  type:object," +
        "  properties: {" +
        "   age : {type: number}," +
        "   adult : {type: boolean}," +
        "   name: {type: string}}}");
    errs = validator.validateJson(Utils.fromJSONString("{name:x, age:21, adult:true}"));
    assertNull(errs);
    errs = validator.validateJson(Utils.fromJSONString("{name:x, age:'21', adult:'true'}"));
    assertNotNull(errs);

    errs = validator.validateJson(Utils.fromJSONString("{name:x, age:'x21', adult:'true'}"));
    assertEquals(1, errs.size());
    Exception e = expectThrows(Exception.class, () -> {
      new JsonSchemaValidator("{" +
          "  type:object," +
          "  properties: {" +
          "   age : {type: int}," +
          "   adult : {type: Boolean}," +
          "   name: {type: string}}}");
    });
    assertTrue(e.getMessage().contains("Unknown type"));

    e = expectThrows(Exception.class, () -> {
      new JsonSchemaValidator("{" +
          "  type:object," +
          "   x : y," +
          "  properties: {" +
          "   age : {type: number}," +
          "   adult : {type: boolean}," +
          "   name: {type: string}}}");
    });
    assertTrue(e.getMessage().contains("Unknown key"));

    e = expectThrows(Exception.class, () -> {
      new JsonSchemaValidator("{" +
          "  type:object," +
          "  propertes: {" +
          "   age : {type: number}," +
          "   adult : {type: boolean}," +
          "   name: {type: string}}}");
    });
    assertTrue(e.getMessage().contains("Unknown key : propertes"));

    validator = new JsonSchemaValidator("{" +
        "  type:object," +
        "  properties: {" +
        "   age : {type: number}," +
        "   sex: {type: string, enum:[M, F]}," +
        "   adult : {type: boolean}," +
        "   name: {type: string}}}");
    errs = validator.validateJson(Utils.fromJSONString("{name: 'Joe Average' , sex:M}"));
    assertNull("errs are " + errs, errs);
    errs = validator.validateJson(Utils.fromJSONString("{name: 'Joe Average' , sex:m}"));
    assertEquals(1, errs.size());
    assertTrue(errs.get(0).contains("Value of enum"));

    String schema = "{\n" +
        "  'type': 'object',\n" +
        "  'properties': {\n" +
        "    'links': {\n" +
        "      'type': 'array',\n" +
        "      'items':{" +
        "          'type': 'object',\n" +
        "          'properties': {\n" +
        "            'rel': {\n" +
        "              'type': 'string'\n" +
        "            },\n" +
        "            'href': {\n" +
        "              'type': 'string'\n" +
        "            }\n" +
        "          }\n" +
        "        }\n" +
        "    }\n" +
        "\n" +
        "  }\n" +
        "}";
    validator = new JsonSchemaValidator(schema);
    validator.validateJson(Utils.fromJSONString("{\n" +
        "  'links': [\n" +
        "    {\n" +
        "        'rel': 'x',\n" +
        "        'href': 'x'\n" +
        "    },\n" +
        "    {\n" +
        "        'rel': 'x',\n" +
        "        'href': 'x'\n" +
        "    },\n" +
        "    {\n" +
        "        'rel': 'x',\n" +
        "        'href': 'x'\n" +
        "    }\n" +
        "  ]\n" +
        "}"));

    schema = "{\n" +
        "'type' : 'object',\n" +
        "'oneOf' : ['a', 'b']\n" +
        "}";
    validator = new JsonSchemaValidator(schema);
    errs = validator.validateJson(Utils.fromJSONString("" +
        "{'c':'val'}"));
    assertNotNull(errs);
    errs = validator.validateJson(Utils.fromJSONString("" +
        "{'a':'val'}"));
    assertNull(errs);

  }

  public void testNullObjectValue() {
    ValidatingJsonMap spec = Utils.getSpec("cluster.Commands").getSpec();
    JsonSchemaValidator validator = new JsonSchemaValidator((Map) Utils.getObjectByPath(spec, false, "/commands/set-obj-property"));
    List<String> object = validator.validateJson(Utils.fromJSONString("{collectionDefaults: null}"));
    assertNull(object);
  }

  @SuppressWarnings("rawtypes")
  private void checkSchema(String name) {
    ValidatingJsonMap spec = Utils.getSpec(name).getSpec();
    Map commands = (Map) spec.get("commands");
    for (Object o : commands.entrySet()) {
      Map.Entry cmd = (Map.Entry) o;
      try {
        JsonSchemaValidator validator = new JsonSchemaValidator((Map) cmd.getValue());
      } catch (Exception e) {
        throw new RuntimeException("Error in command  " + cmd.getKey() + " in schema " + name, e);
      }
    }
  }

}
