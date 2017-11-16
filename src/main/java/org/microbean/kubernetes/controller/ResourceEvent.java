/* -*- mode: Java; c-basic-offset: 2; indent-tabs-mode: nil; coding: utf-8-unix -*-
 *
 * Copyright © 2017 MicroBean.
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

public class ResourceEvent extends EventObject {

  private static final long serialVersionUID = 1L;

  private final Type type;
  
  private final Object oldObject;

  private final Object newObject;
  
  public ResourceEvent(final Object source, final Type type, final Object oldObject, final Object newObject) {
    super(source);
    this.type = Objects.requireNonNull(type);
    this.oldObject = oldObject;
    this.newObject = newObject;
  }

  public final Type getType() {
    return this.type;
  }
  
  public final Object getOldObject() {
    return this.oldObject;
  }

  public final Object getNewObject() {
    return this.newObject;
  }

  public enum Type {
    ADDITION,
    UPDATE,
    DELETION
  }
  
}
