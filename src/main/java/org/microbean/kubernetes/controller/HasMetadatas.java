/* -*- mode: Java; c-basic-offset: 2; indent-tabs-mode: nil; coding: utf-8-unix -*-
 *
 * Copyright © 2017-2018 microBean.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.microbean.kubernetes.controller;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;

public final class HasMetadatas {

  private HasMetadatas() {
    super();
  }
  
  public static final Object getKey(final HasMetadata resource) {
    final Object returnValue;
    if (resource == null) {
      returnValue = null;
    } else {
      final ObjectMeta metadata = resource.getMetadata();
      if (metadata == null) {
        returnValue = null;
      } else {
        String name = metadata.getName();
        if (name == null) {
          throw new IllegalStateException("metadata.getName() == null");
        } else if (name.isEmpty()) {
          throw new IllegalStateException("metadata.getName().isEmpty()");
        }
        final String namespace = metadata.getNamespace();
        if (namespace == null || namespace.isEmpty()) {
          returnValue = name;
        } else {
          returnValue = new StringBuilder(namespace).append("/").append(name).toString();
        }
      }
    }
    return returnValue;
  }
  
}
