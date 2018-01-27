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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;

import java.util.function.Consumer;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;

import org.microbean.development.annotation.Blocking;
import org.microbean.development.annotation.NonBlocking;

public final class EventQueueCollection<T extends HasMetadata> implements EventCache<T>, Iterable<EventQueue<T>> {

  private volatile boolean closing;
  
  private volatile boolean populated;

  private volatile int initialPopulationCount;

  private volatile Thread consumerThread;
  
  private final LinkedHashMap<Object, EventQueue<T>> map;

  private final Map<?, ? extends T> knownObjects;
  
  public EventQueueCollection(final Map<?, ? extends T> knownObjects, final int initialCapacity, final float loadFactor) {
    super();
    this.map = new LinkedHashMap<>(initialCapacity, loadFactor);
    this.knownObjects = knownObjects;
  }

  private final Map<?, ? extends T> getKnownObjects() {
    return this.knownObjects;
  }

  private synchronized final boolean isEmpty() {
    return this.map.isEmpty();
  }
  
  @Override
  public synchronized final Iterator<EventQueue<T>> iterator() {
    return this.map.values().iterator();
  }

  @Override
  public synchronized final Spliterator<EventQueue<T>> spliterator() {
    return this.map.values().spliterator();
  }

  @Override
  public synchronized final void forEach(final Consumer<? super EventQueue<T>> action) {
    if (action != null && !this.isEmpty()) {
      Iterable.super.forEach(action);
    }
  }

  synchronized final boolean getHasSynced() {
    return this.populated && this.initialPopulationCount == 0;
  }

  @Override
  public synchronized final void resync() {
    final Map<?, ? extends T> knownObjects = this.getKnownObjects();
    if (knownObjects != null) {
      synchronized (knownObjects) {
        if (!knownObjects.isEmpty()) {
          final Collection<? extends T> values = knownObjects.values();
          if (values != null && !values.isEmpty()) {
            for (final T knownObject : values) {
              if (knownObject != null) {
                // We follow the Go code in that we use *our* key
                // extraction logic, rather than relying on the known
                // key in the knownObjects map.
                final Object key = this.getKey(knownObject);
                if (key != null) {
                  final EventQueue<T> eventQueue = this.map.get(key);
                  if (eventQueue == null || eventQueue.isEmpty()) {
                    this.add(new Event<>(this, Event.Type.SYNCHRONIZATION, null, knownObject));
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  @Override
  public synchronized final void replace(final Collection<? extends T> incomingResources, final Object resourceVersion) {
    Objects.requireNonNull(incomingResources);
    final Set<Object> replacementKeys;
    if (incomingResources.isEmpty()) {
      replacementKeys = Collections.emptySet();
    } else {
      replacementKeys = new HashSet<>();
      for (final T resource : incomingResources) {
        if (resource != null) {
          final Object replacementKey = this.getKey(resource);
          if (replacementKey == null) {
            throw new IllegalStateException("getKey(resource) == null: " + resource);
          }
          replacementKeys.add(replacementKey);
          this.add(new Event<>(this, Event.Type.SYNCHRONIZATION, null, resource), false);
        }
      }
    }

    int queuedDeletions = 0;
    
    final Map<?, ? extends T> knownObjects = this.getKnownObjects();
    if (knownObjects == null) {

      for (final EventQueue<T> eventQueue : this.map.values()) {
        assert eventQueue != null;
        synchronized (eventQueue) {
          if (eventQueue.isEmpty()) {
            assert false : "eventQueue.isEmpty(): " + eventQueue;
          } else {
            final Object key = eventQueue.getKey();
            if (!replacementKeys.contains(key)) {
              // We have an event queue indexed under a key that no
              // longer exists in Kubernetes.
              final Event<? extends T> newestEvent = eventQueue.getLast();
              assert newestEvent != null;
              final T resourceToBeDeleted = newestEvent.getResource();
              assert resourceToBeDeleted != null;
              this.add(new Event<>(this, Event.Type.DELETION, key, resourceToBeDeleted), false);
            }
          }
        }
      }
      
    } else {

      synchronized (knownObjects) {
        if (!knownObjects.isEmpty()) {
          final Collection<? extends Entry<?, ? extends T>> entrySet = knownObjects.entrySet();
          if (entrySet != null && !entrySet.isEmpty()) {
            for (final Entry<?, ? extends T> entry : entrySet) {
              if (entry != null) {
                final Object knownKey = entry.getKey();
                if (!replacementKeys.contains(knownKey)) {
                  this.add(new Event<>(this, Event.Type.DELETION, knownKey, entry.getValue()), false);
                  queuedDeletions++;
                }
              }
            }
          }
        }
      }
      
    }
      
    if (!this.populated) {
      this.populated = true;
      this.initialPopulationCount = incomingResources.size() + queuedDeletions;
    }
    
  }

  protected Object getKey(final T resource) {
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

  protected EventQueue<T> createEventQueue(final Object key) {
    return new EventQueue<T>(key);
  }

  @NonBlocking
  public final void start(final Consumer<? super EventQueue<? extends T>> siphon) {
    Objects.requireNonNull(siphon);
    synchronized (this) {
      if (this.consumerThread == null) {
        this.consumerThread = new Thread(() -> {
            try {
              attach(siphon);
            } catch (final InterruptedException interruptedException) {
              Thread.currentThread().interrupt();
            }
          });
        this.consumerThread.setDaemon(true);
        this.consumerThread.start();
      }
    }
  }

  public final void close() {
    final Thread consumerThread;
    synchronized (this) {
      consumerThread = this.consumerThread;
      if (consumerThread != null) {
        this.closing = true;
        this.consumerThread.interrupt();
      }
    }
    if (consumerThread != null) {
      try {
        consumerThread.join();
      } catch (final InterruptedException interruptedException) {
        Thread.currentThread().interrupt();
      } finally {
        this.consumerThread = null;
        this.closing = false;
      }
    }
  }

  /**
   * <strong>Blocks the current thread</strong> and removes and
   * funnels current and future {@link EventQueue}s belonging to this
   * {@link EventQueueCollection} into the supplied {@link Consumer},
   * blocking if necessary until there are new {@link EventQueue}s in
   * this {@link EventQueueCollection} for the supplied {@link
   * Consumer} to consume.
   *
   * <p>The supplied {@link Consumer} may call any method on the
   * {@link EventQueue} supplied to it without synchronization as the
   * {@link EventQueue}'s monitor is obtained by the {@link Thread}
   * invoking the {@link Consumer}.</p>
   *
   * @param siphon a {@link Consumer} that can process an {@link
   * EventQueue} or its superclasses; must not be {@code null}
   *
   * @exception NullPointerException if {@code consumer} is {@code null}
   *
   * @exception InterruptedException if the {@link Thread} currently
   * occupied with this method invocation was {@linkplain
   * Thread#interrupt() interrupted}
   */
  @Blocking
  private final void attach(final Consumer<? super EventQueue<? extends T>> siphon) throws InterruptedException {
    Objects.requireNonNull(siphon);
    while (!this.closing) {
      synchronized (this) {
        @Blocking
        final EventQueue<T> eventQueue = this.take();
        if (eventQueue != null) {
          synchronized (eventQueue) {
            try {
              siphon.accept(eventQueue);
            } catch (final RuntimeException runtimeException) {
              // TODO: catch a subclass that indicates a requeue request;
              throw runtimeException;
            }
          }
        }
      }
    }
  }

  @Blocking
  private synchronized final EventQueue<T> take() throws InterruptedException {
    while (!this.closing && this.isEmpty()) {
      try {
        this.wait();
      } catch (final InterruptedException interruptedException) {
        if (this.closing) {
          throw interruptedException;
        }
        Thread.currentThread().interrupt();
      }
    }
    final EventQueue<T> returnValue;
    if (this.closing) {
      returnValue = null;
    } else {
      assert !this.isEmpty();
      final Iterator<EventQueue<T>> iterator = this.map.values().iterator();
      assert iterator != null;
      assert iterator.hasNext();
      returnValue = iterator.next();
      assert returnValue != null;
      iterator.remove();
      Thread.interrupted(); // clear any interrupted status now that we know we're going to be successful
    }
    return returnValue;
  }

  @Override
  public final boolean add(final Event<T> event) {
    return this.add(event, true);
  }

  private final boolean add(final Event<T> event, final boolean populate) {
    Objects.requireNonNull(event);
    final Object key = event.getKey();
    if (key == null) {
      throw new IllegalArgumentException("event.getKey() == null");
    }
    if (populate) {
      this.populated = true;
    }
    
    boolean returnValue = false;
    EventQueue<T> eventQueue;
    synchronized (this) {
      eventQueue = this.map.get(key);
      final boolean eventQueueExisted = eventQueue != null;
      if (eventQueue == null) {
        eventQueue = this.createEventQueue(key);
        if (eventQueue == null) {
          throw new IllegalStateException("createEventQueue(key) == null: " + key);
        }
      }
      synchronized (eventQueue) {
        returnValue = eventQueue.add(event);
        if (eventQueue.isEmpty()) {
          // Compression might have emptied the queue, so an add could
          // result in an empty queue.
          if (eventQueueExisted) {
            this.map.remove(key);
          }
        } else if (!eventQueueExisted) {
          this.map.put(key, eventQueue);
        }
      }
      this.notifyAll();
    }
    return returnValue;
  }

}
