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

import java.util.EventObject;
import java.util.Objects;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;

public class Event<T extends HasMetadata> extends EventObject {

  private static final long serialVersionUID = 1L;
  
  Object key;
  
  private final Type type;
  
  private final T resource;

  public Event(final Object source, final Type type, final T resource) {
    this(source, type, null, resource);
  }
  
  Event(final Object source, final Type type, final Object key, final T resource) {
    super(source);
    this.type = Objects.requireNonNull(type);
    this.key = key;
    this.resource = Objects.requireNonNull(resource);
  }

  public final Type getType() {
    return this.type;
  }
  
  public final T getResource() {
    return this.resource;
  }

  public final boolean isFinalStateKnown() {
    return this.key == null;
  }

  final void setKey(final Object key) {
    this.key = key;
  }
  
  public Object getKey() {
    Object returnValue = this.key;
    if (returnValue == null) {
      final T resource = this.getResource();
      if (resource != null) {
        final ObjectMeta metadata = resource.getMetadata();
        if (metadata != null) {
          final String namespace = metadata.getNamespace();
          if (namespace == null || namespace.isEmpty()) {
            returnValue = metadata.getName();
          } else {
            returnValue = new StringBuilder(namespace).append("/").append(metadata.getName()).toString();
          }
        }
      }
    }
    return returnValue;
  }

  @Override
  public int hashCode() {
    int hashCode = 37;
    
    final Object source = this.getSource();
    int c = source == null ? 0 : source.hashCode();
    hashCode = hashCode * 17 + c;
    
    final Object key = this.getKey();
    c = key == null ? 0 : key.hashCode();
    hashCode = hashCode * 17 + c;
    
    final Object type = this.getType();
    c = type == null ? 0 : type.hashCode();
    hashCode = hashCode * 17 + c;
    
    final Object resource = this.getResource();
    c = resource == null ? 0 : resource.hashCode();
    hashCode = hashCode * 17 + c;
    
    return hashCode;
  }

  @Override
  public boolean equals(final Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof Event) {
      
      final Event<?> her = (Event<?>)other;
      
      final Object source = this.getSource();
      if (source == null) {
        if (her.getSource() != null) {
          return false;
        }
      } else if (!source.equals(her.getSource())) {
        return false;
      }
      
      final Object key = this.getKey();
      if (key == null) {
        if (her.getKey() != null) {
          return false;
        }
      } else if (!key.equals(her.getKey())) {
        return false;
      }
      
      final Object type = this.getType();
      if (type == null) {
        if (her.getType() != null) {
          return false;
        }
      } else if (!type.equals(her.getType())) {
        return false;
      }
      
      final Object resource = this.getResource();
      if (resource == null) {
        if (her.getResource() != null) {
          return false;
        }
      } else if (!resource.equals(her.getResource())) {
        return false;
      }

      return true;
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return new StringBuilder().append(this.getType()).append(": ").append(this.getResource()).toString();
  }
  
  public static enum Type {
    ADDITION,
    MODIFICATION,
    DELETION,
    SYNCHRONIZATION      
  }
}
