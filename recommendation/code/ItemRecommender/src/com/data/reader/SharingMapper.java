/**
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

package com.data.reader;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public abstract class SharingMapper<K1,V1,K2,V2,S> extends Mapper<K1,V1,K2,V2> {

  private static Object SHARED_INSTANCE;


  abstract S createSharedInstance(Context context) throws IOException;

  final void setupSharedInstance(Context context) throws IOException {
    if (SHARED_INSTANCE == null) {
      SHARED_INSTANCE = createSharedInstance(context);
    }
  }

  final S getSharedInstance() {
    return (S) SHARED_INSTANCE;
  }

  static void reset() {
    SHARED_INSTANCE = null;
  }
}
