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

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

public class SecureRandomAlgorithmTesterApp {
  public static void main(String[] args) throws NoSuchAlgorithmException {
    String algorithm = args[0];
    String method = args[1];
    int amount = Integer.valueOf(args[2]);
    SecureRandom secureRandom;
    if(algorithm.equals("default"))
      secureRandom = new SecureRandom();
    else 
      secureRandom = SecureRandom.getInstance(algorithm);
    System.out.println("Algorithm:" + secureRandom.getAlgorithm());
    switch(method) {
      case "seed": secureRandom.generateSeed(amount); break;
      case "bytes": secureRandom.nextBytes(new byte[amount]); break;
      case "long": secureRandom.nextLong(); break;
      case "int": secureRandom.nextInt(); break;
      default: throw new IllegalArgumentException("Not supported random function: " + method);
    }
    System.out.println("SecureRandom function invoked");
  }
}
