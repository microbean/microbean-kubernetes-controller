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

import java.util.List;
import java.util.Objects;

import java.util.function.Function;
import java.util.function.Supplier;

import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;

public class SharedInformer<T> implements Runnable {

  private final Store<T> store;

  private boolean hasSynced;

  private volatile String lastSyncResourceVersion;
  
  public SharedInformer(final Store<T> store,
                        final Supplier<? extends List<? extends T>> listFunction,
                        final Function<Watcher<T>, Watch> watchFunction) {
    super();
    this.store = Objects.requireNonNull(store);
  }

  public final Store<T> getStore() {
    return this.store;
  }

  public void run() {
    // new delta fifo
    
  }

  public boolean getHasSynced() {
    return this.hasSynced;
  }

  public String getLastSyncResourceVersion() {
    return this.lastSyncResourceVersion;
  }
  
}
