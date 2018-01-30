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

import java.io.Closeable;
import java.io.IOException;

import java.time.Duration;

import java.util.Map;
import java.util.Objects;

import java.util.concurrent.ScheduledExecutorService;

import java.util.function.Consumer;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;

import io.fabric8.kubernetes.client.Watcher;

import io.fabric8.kubernetes.client.dsl.Listable;
import io.fabric8.kubernetes.client.dsl.VersionWatchable;

import net.jcip.annotations.Immutable;

@Immutable
public class Controller<T extends HasMetadata> {

  private final Reflector<T> reflector;

  private final EventQueueCollection<T> eventCache;

  private final Consumer<? super EventQueue<? extends T>> siphon;

  @SuppressWarnings("rawtypes")
  public <X extends Listable<? extends KubernetesResourceList> & VersionWatchable<? extends Closeable, Watcher<T>>> Controller(final X operation,
                                                                                                                               final Consumer<? super EventQueue<? extends T>> siphon) {
    this(operation, null, null, null, siphon);
  }
  
  @SuppressWarnings("rawtypes")
  public <X extends Listable<? extends KubernetesResourceList> & VersionWatchable<? extends Closeable, Watcher<T>>> Controller(final X operation,
                                                                                                                               final Map<Object, T> knownObjects,
                                                                                                                               final Consumer<? super EventQueue<? extends T>> siphon) {
    this(operation, null, null, knownObjects, siphon);
  }
  
  @SuppressWarnings("rawtypes")
  public <X extends Listable<? extends KubernetesResourceList> & VersionWatchable<? extends Closeable, Watcher<T>>> Controller(final X operation,
                                                                                                                               final Duration synchronizationInterval,
                                                                                                                               final Consumer<? super EventQueue<? extends T>> siphon) {
    this(operation, null, synchronizationInterval, null, siphon);
  }

  @SuppressWarnings("rawtypes")
  public <X extends Listable<? extends KubernetesResourceList> & VersionWatchable<? extends Closeable, Watcher<T>>> Controller(final X operation,
                                                                                                                               final Duration synchronizationInterval,
                                                                                                                               final Map<Object, T> knownObjects,
                                                                                                                               final Consumer<? super EventQueue<? extends T>> siphon) {
    this(operation, null, synchronizationInterval, knownObjects, siphon);
  }

  @SuppressWarnings("rawtypes")
  public <X extends Listable<? extends KubernetesResourceList> & VersionWatchable<? extends Closeable, Watcher<T>>> Controller(final X operation,
                                                                                                                               final ScheduledExecutorService synchronizationExecutorService,
                                                                                                                               final Duration synchronizationInterval,
                                                                                                                               final Map<Object, T> knownObjects,
                                                                                                                               final Consumer<? super EventQueue<? extends T>> siphon) {
    super();
    this.siphon = Objects.requireNonNull(siphon);
    this.eventCache = new ControllerEventQueueCollection(knownObjects);
    this.reflector = new ControllerReflector(operation, synchronizationExecutorService, synchronizationInterval);
  }

  public final void start() {
    this.eventCache.start(siphon);
    this.reflector.start();
  }

  public final void close() throws IOException {
    Exception throwMe = null;
    try {
      this.reflector.close();
    } catch (final Exception everything) {
      throwMe = everything;
    }

    try {
      this.eventCache.close();
    } catch (final RuntimeException runtimeException) {
      if (throwMe == null) {
        throw runtimeException;
      }
      assert throwMe instanceof IOException;
      throwMe.addSuppressed(runtimeException);
      throw (IOException)throwMe;
    }
  }

  protected boolean shouldSynchronize() {
    return true;
  }

  protected void onClose() {

  }

  protected Object getKey(final T resource) {
    return HasMetadatas.getKey(resource);
  }

  protected Event<T> createEvent(final Object source, final Event.Type eventType, final T resource) {
    return new Event<T>(source, eventType, null, resource);
  }

  protected EventQueue<T> createEventQueue(final Object key) {
    return new EventQueue<T>(key);
  }

  private final class ControllerEventQueueCollection extends EventQueueCollection<T> {

    private ControllerEventQueueCollection(final Map<?, ? extends T> knownObjects) {
      super(knownObjects);
    }
    
    @Override
    protected final Event<T> createEvent(final Object source, final Event.Type eventType, final T resource) {
      return Controller.this.createEvent(source, eventType, resource);
    }
    
    @Override
    protected final EventQueue<T> createEventQueue(final Object key) {
      return Controller.this.createEventQueue(key);
    }
    
    @Override
    protected final Object getKey(final T resource) {
      return Controller.this.getKey(resource);
    }
    
  }
    
  private final class ControllerReflector extends Reflector<T> {

    @SuppressWarnings("rawtypes")
    private <X extends Listable<? extends KubernetesResourceList> & VersionWatchable<? extends Closeable, Watcher<T>>> ControllerReflector(final X operation,
                                                                                                                                           final ScheduledExecutorService synchronizationExecutorService,
                                                                                                                                           final Duration synchronizationInterval) {
      super(operation, Controller.this.eventCache, synchronizationExecutorService, synchronizationInterval);
    }
    
    @Override
    protected final boolean shouldSynchronize() {
      return Controller.this.shouldSynchronize();
    }

    @Override
    protected final void onClose() {
      Controller.this.onClose();
    }
  }
  
}
