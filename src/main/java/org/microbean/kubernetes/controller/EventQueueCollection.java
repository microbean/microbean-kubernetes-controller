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

/**
 * An {@link EventCache} that temporarily stores {@link Event}s in
 * {@link EventQueue}s, one per named Kubernetes resource, and
 * {@linkplain #start(Consumer) provides a means for processing those
 * queues}.
 *
 * @param <T> a type of Kubernetes resource
 *
 * @author <a href="https://about.me/lairdnelson"
 * target="_parent">Laird Nelson</a>
 *
 * @see #add(Object, Event.Type, HasMetadata)
 *
 * @see #replace(Collection, Object)
 *
 * @see #synchronize()
 *
 * @see #start(Consumer)
 */
public final class EventQueueCollection<T extends HasMetadata> implements EventCache<T>, Iterable<EventQueue<T>> {


  /*
   * Instance fields.
   */


  /**
   * Whether this {@link EventQueueCollection} is in the process of
   * {@linkplain #close() closing}.
   *
   * @see #close()
   */
  private volatile boolean closing;

  /**
   * Whether or not this {@link EventQueueCollection} has been
   * populated via an invocation of the {@link #replace(Collection,
   * Object)} method.
   *
   * <p>Mutations of this field must be synchronized on {@code
   * this}.</p>
   *
   * @see #replace(Collection, Object)
   */
  private boolean populated;

  /**
   * The number of {@link EventQueue}s that this {@link
   * EventQueueCollection} was initially {@linkplain
   * #replace(Collection, Object) seeded with}.
   *
   * <p>Mutations of this field must be synchronized on {@code
   * this}.</p>
   *
   * @see #replace(Collection, Object)
   */
  private int initialPopulationCount;

  /**
   * A {@link Thread}, created by the {@link #start(Consumer)} method,
   * that will eternally invoke the {@link #attach(Consumer)} method.
   *
   * <p>This field may be {@code null}.</p>
   *
   * <p>Mutations of this field must be synchronized on {@code
   * this}.</p>
   *
   * @see #start(Consumer)
   *
   * @see #attach(Consumer)
   */
  private Thread consumerThread;

  /**
   * A {@link LinkedHashMap} of {@link EventQueue} instances, indexed
   * by {@linkplain EventQueue#getKey() their keys}.
   *
   * <p>This field is never {@code null}.</p>
   *
   * <p>Mutations of the contents of this {@link LinkedHashMap} must
   * be synchronized on {@code this}.</p>
   *
   * @see #add(Object, Event.Type, Resource)
   */
  private final LinkedHashMap<Object, EventQueue<T>> map;

  /**
   * A {@link Map} containing the last known state of Kubernetes
   * resources this {@link EventQueueCollection} is caching events
   * for.  This field is used chiefly by the {@link #synchronize()}
   * method, but by others as well.
   *
   * <p>This field may be {@code null}.</p>
   *
   * <p>Mutations of this field must be synchronized on this field's
   * value.</p>
   *
   * @see #getKnownObjects()
   *
   * @see #synchronize()
   */
  private final Map<?, ? extends T> knownObjects;


  /*
   * Constructors.
   */


  /**
   * Creates a new {@link EventQueueCollection} with an initial
   * capacity of {@code 16} and a load factor of {@code 0.75} that is
   * not interested in tracking Kubernetes resource deletions.
   *
   * @see #EventQueueCollection(Map, int, float)
   */
  public EventQueueCollection() {
    this(null, 16, 0.75f);
  }

  /**
   * Creates a new {@link EventQueueCollection} with an initial
   * capacity of {@code 16} and a load factor of {@code 0.75}.
   *
   * @param knownObjects a {@link Map} containing the last known state
   * of Kubernetes resources this {@link EventQueueCollection} is
   * caching events for; may be {@code null} if this {@link
   * EventQueueCollection} is not interested in tracking deletions of
   * objects; if non-{@code null} <strong>will be synchronized on by
   * this class</strong> during retrieval and traversal operations
   *
   * @see #EventQueueCollection(Map, int, float)
   */
  public EventQueueCollection(final Map<?, ? extends T> knownObjects) {
    this(knownObjects, 16, 0.75f);
  }

  /**
   * Creates a new {@link EventQueueCollection}.
   *
   * @param knownObjects a {@link Map} containing the last known state
   * of Kubernetes resources this {@link EventQueueCollection} is
   * caching events for; may be {@code null} if this {@link
   * EventQueueCollection} is not interested in tracking deletions of
   * objects; if non-{@code null} <strong>will be synchronized on by
   * this class</strong> during retrieval and traversal operations
   *
   * @param initialCapacity the initial capacity of the internal data
   * structure used to house this {@link EventQueueCollection}'s
   * {@link EventQueue}s; must be an integer greater than {@code 0}
   *
   * @param loadFactor the load factor of the internal data structure
   * used to house this {@link EventQueueCollection}'s {@link
   * EventQueue}s; must be a positive number between {@code 0} and
   * {@code 1}
   */
  public EventQueueCollection(final Map<?, ? extends T> knownObjects, final int initialCapacity, final float loadFactor) {
    super();
    this.map = new LinkedHashMap<>(initialCapacity, loadFactor);
    this.knownObjects = knownObjects;
  }


  /*
   * Instance methods.
   */

  
  private final Map<?, ? extends T> getKnownObjects() {
    return this.knownObjects;
  }

  private synchronized final boolean isEmpty() {
    return this.map.isEmpty();
  }

  /**
   * Returns an {@link Iterator} of {@link EventQueue} instances that
   * are created and managed by this {@link EventQueueCollection} as a
   * result of the {@link #add(Object, Event.Type, HasMetadata)},
   * {@link #replace(Collection, Object)} and {@link #synchronize()}
   * methods.
   *
   * <p>This method never returns {@code null}.</p>
   *
   * <p>The returned {@link Iterator} implements the {@link
   * Iterator#remove()} method properly.</p>
   *
   * @return a non-{@code null} {@link Iterator} of {@link
   * EventQueue}s
   */
  @Override
  public synchronized final Iterator<EventQueue<T>> iterator() {
    return this.map.values().iterator();
  }

  /**
   * Returns a {@link Spliterator} of {@link EventQueue} instances
   * that are created and managed by this {@link EventQueueCollection}
   * as a result of the {@link #add(Object, Event.Type, HasMetadata)},
   * {@link #replace(Collection, Object)} and {@link #synchronize()}
   * methods.
   *
   * <p>This method never returns {@code null}.</p>
   *
   * @return a non-{@code null} {@link Spliterator} of {@link
   * EventQueue}s
   *
   * @see Map#spliterator()
   */
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
  public synchronized final void synchronize() {
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
                    this.add(this, Event.Type.SYNCHRONIZATION, knownObject);
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
          this.add(this, Event.Type.SYNCHRONIZATION, resource, false);
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
              final Event<T> event = this.createEvent(this, Event.Type.DELETION, resourceToBeDeleted);
              if (event == null) {
                throw new IllegalStateException("createEvent() == null");
              }
              event.setKey(key);
              this.add(event, false);
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
                  final Event<T> event = this.createEvent(this, Event.Type.DELETION, entry.getValue());
                  if (event == null) {
                    throw new IllegalStateException("createEvent() == null");
                  }
                  event.setKey(knownKey);
                  this.add(event, false);
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
        synchronized (this) {
          this.consumerThread = null;
          this.closing = false;
        }
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

  protected Event<T> createEvent(final Object source, final Event.Type eventType, final T resource) {
    Objects.requireNonNull(source);
    Objects.requireNonNull(eventType);
    Objects.requireNonNull(resource);
    return new Event<>(source, eventType, resource);
  }
  
  @Override
  public final Event<T> add(final Object source, final Event.Type eventType, final T resource) {
    return this.add(source, eventType, resource, true);
  }

  private final Event<T> add(final Object source, final Event.Type eventType, final T resource, final boolean populate) {
    final Event<T> event = this.createEvent(source, eventType, resource);
    if (event == null) {
      throw new IllegalStateException("createEvent() == null");
    }
    return this.add(event, populate);
  }

  private final Event<T> add(final Event<T> event, final boolean populate) {
    Objects.requireNonNull(event);
    final Object key = event.getKey();
    if (key == null) {
      throw new IllegalArgumentException("event.getKey() == null");
    }
    if (populate) {
      this.populated = true;
    }
    
    Event<T> returnValue = null;
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
        if (eventQueue.add(event)) {
          returnValue = event;
        }
        if (eventQueue.isEmpty()) {
          // Compression might have emptied the queue, so an add could
          // result in an empty queue.
          if (eventQueueExisted) {
            returnValue = null;
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
