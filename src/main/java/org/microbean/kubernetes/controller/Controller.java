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

import java.time.Duration;

import java.util.Objects;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import java.util.function.BiConsumer;

import io.fabric8.kubernetes.client.dsl.MixedOperation;

import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.HasMetadata;

public class Controller<T extends HasMetadata, L extends KubernetesResourceList, D> implements Runnable, Syncable {

  private final QueueStore<T, D> queueStore;
  
  private final Reflector<T, L, D> reflector;

  private final boolean retryOnError;
  
  private final BiConsumer<QueueStore<T, D>, D> processor;

  private final ExecutorService reflectorExecutorService;

  private volatile boolean stop;
  
  protected Controller(final MixedOperation<T, L, ?, ?> operation,
                       final ExecutorService reflectorExecutorService,
                       final ScheduledExecutorService resyncExecutorService,
                       final Duration resyncInterval,
                       final QueueStore<T, D> queueStore,
                       final BiConsumer<QueueStore<T, D>, D> processor,
                       final boolean retryOnError) {
    super();
    this.queueStore = Objects.requireNonNull(queueStore);
    this.reflector = new Reflector<T, L, D>(queueStore, operation, resyncExecutorService, resyncInterval) {
        @Override
        protected final boolean shouldResync() {
          return Controller.this.shouldResync();
        }
      };
    this.reflectorExecutorService = Objects.requireNonNull(reflectorExecutorService);
    this.processor = Objects.requireNonNull(processor);
    this.retryOnError = retryOnError;
  }

  @Override
  public final boolean getHasSynced() {
    return this.queueStore.getHasSynced();
  }

  @Override
  public final String getLastSyncResourceVersion() {
    return this.reflector.getLastSyncResourceVersion();
  }  

  protected boolean shouldResync() {
    return true;
  }

  public final boolean getRetryOnError() {
    return this.retryOnError;
  }

  public final void close() {
    this.stop = true;    
  }
  
  @Override
  public final void run() {
    if (!this.stop) {
      final Future<?> reflectorJob = this.reflectorExecutorService.submit(this.reflector);
      this.processLoop(reflectorJob);
    }
  }

  // Modeled after the processLoop function in controller.go.  That
  // function swallows all errors.  I'm not sure why.
  private final void processLoop(final Future<?> reflectorJob) {
    while (!this.stop) {
      D processedObject = null;
      try {
        processedObject = this.queueStore.popAndProcessUsing(this.processor);
      } catch (final InterruptedException interruptedException) {
        if (reflectorJob != null) {
          reflectorJob.cancel(true);
        }
        this.reflector.close();
        Thread.currentThread().interrupt();       
        this.stop = true;
      } catch (final RuntimeException runtimeException) {
        if (this.retryOnError) {
          this.queueStore.addIfNotPresent(processedObject);
        } else {
          // Er, the Go code ignores all errors here....
        }
      }
    }
  }
  
}
