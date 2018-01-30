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

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

import org.microbean.development.annotation.Blocking;
import org.microbean.development.annotation.NonBlocking;

/**
 * An {@link EventCache} that temporarily stores {@link Event}s in
 * {@link EventQueue}s, one per named Kubernetes resource, and
 * {@linkplain #start(Consumer) provides a means for processing those
 * queues}.
 *
 * <h2>Thread Safety</h2>
 *
 * <p>This class is safe for concurrent use by multiple {@link
 * Thread}s.  Some operations, like the usage of the {@link
 * #iterator()} method, require that callers synchronize on the {@link
 * EventQueue} directly.  This class' internals synchronize on {@code
 * this} when locking is needed.</p>
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
 *
 * @see EventQueue
 */
@ThreadSafe
public class EventQueueCollection<T extends HasMetadata> implements EventCache<T>, Iterable<EventQueue<T>>, AutoCloseable {


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
  @GuardedBy("this")
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
  @GuardedBy("this")
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
  @GuardedBy("this")
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
  @GuardedBy("this")
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
  @GuardedBy("itself")
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
   * <p>To call this method and then use this {@link Iterator} you
   * must synchronize on this {@link EventQueueCollection}.</p>
   *
   * <p>This method never returns {@code null}.</p>
   *
   * <p>The returned {@link Iterator}'s {@link Iterator#remove()}
   * method throws an {@link UnsupportedOperationException}.</p>
   *
   * @return a non-{@code null} {@link Iterator} of {@link
   * EventQueue}s
   */
  @Override
  public synchronized final Iterator<EventQueue<T>> iterator() {
    return Collections.unmodifiableMap(this.map).values().iterator();
  }

  /**
   * Returns a {@link Spliterator} of {@link EventQueue} instances
   * that are created and managed by this {@link EventQueueCollection}
   * as a result of the {@link #add(Object, Event.Type, HasMetadata)},
   * {@link #replace(Collection, Object)} and {@link #synchronize()}
   * methods.
   *
   * <p>To call this method and then use this {@link Spliterator} you
   * must synchronize on this {@link EventQueueCollection}.</p>
   *
   * <p>This method never returns {@code null}.</p>
   *
   * @return a non-{@code null} {@link Spliterator} of {@link
   * EventQueue}s
   *
   * @see Collection#spliterator()
   */
  @Override
  public synchronized final Spliterator<EventQueue<T>> spliterator() {
    return this.map.values().spliterator();
  }

  /**
   * Invokes the {@link Consumer#accept(Object)} method for each
   * {@link EventQueue} stored by this {@link EventQueueCollection}.
   *
   * @param a {@link Consumer} of {@link EventQueue}s; may be {@code
   * null} in which case no action will be taken
   *
   * @see Iterable#forEach(Consumer)
   */
  @Override
  public synchronized final void forEach(final Consumer<? super EventQueue<T>> action) {
    if (action != null && !this.isEmpty()) {
      Iterable.super.forEach(action);
    }
  }

  /**
   * Returns {@code true} if this {@link EventQueueCollection} has
   * been populated via a call to {@link #add(Object, Event.Type,
   * HasMetadata)} at some point, and if there are no {@link
   * EventQueue}s remaining to be {@linkplain #start(Consumer)
   * removed}.
   *
   * @see #replace(Collection, Object)
   *
   * @see #add(Object, Event.Type, HasMetadata)
   *
   * @see #synchronize()
   */
  public synchronized final boolean isSynchronized() {
    return this.populated && this.initialPopulationCount == 0;
  }

  /**
   * <strong>Synchronizes on the {@code knownObjects} object</strong>
   * {@linkplain #EventQueueCollection(Map, int, float) supplied at
   * construction time}, if there is one, and, for every Kubernetes
   * resource found within at the time of this call, {@linkplain
   * #add(Object, Event.Type, HasMetadata) adds an <code>Event</code>}
   * with an {@link Event.Type} of {@link Event.Type#SYNCHRONIZATION}
   * for it.
   */
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

  /**
   * At a high level, fully replaces the internal state of this {@link
   * EventQueueCollection} to reflect only the Kubernetes resources
   * contained in the supplied {@link Collection}, notionally firing
   * {@link Event}s of type {@link Event.Type#SYNCHRONIZATION} and
   * {@link Event.Type#DELETION} as appropriate.
   *
   * <p>{@link EventQueue}s managed by this {@link
   * EventQueueCollection} that have not yet {@linkplain
   * #start(Consumer) been processed} are not removed by this
   * operation.</p>
   *
   * <p><strong>This method synchronizes on the supplied {@code
   * incomingResources} {@link Collection}</strong> while iterating
   * over it.</p>
   *
   * @param incomingResources the {@link Collection} of Kubernetes
   * resources with which to replace this {@link
   * EventQueueCollection}'s internal state; may be {@code null} or
   * {@linkplain Collection#isEmpty() empty}, which will be taken as
   * an indication that this {@link EventQueueCollection} should
   * effectively be emptied
   *
   * @param resourceVersion the version of the Kubernetes list
   * resource that contained the incoming resources; currently
   * ignored; may be {@code null}
   *
   * @exception IllegalStateException if the {@link
   * #createEvent(Object, Event.Type, HasMetadata)} method returns
   * {@code null} for any reason
   *
   * @see Event.Type#SYNCHRONIZATION
   *
   * @see #createEvent(Object, Event.Type, HasMetadata)
   */
  @Override
  public synchronized final void replace(final Collection<? extends T> incomingResources, final Object resourceVersion) {
    
    final int size;
    final Set<Object> replacementKeys;

    if (incomingResources == null) {
      size = 0;
      replacementKeys = Collections.emptySet();
    } else {
      synchronized (incomingResources) {
        if (incomingResources.isEmpty()) {
          size = 0;
          replacementKeys = Collections.emptySet();
        } else {
          size = incomingResources.size();
          assert size > 0;
          replacementKeys = new HashSet<>();
          for (final T resource : incomingResources) {
            if (resource != null) {
              replacementKeys.add(this.getKey(resource));
              this.add(this, Event.Type.SYNCHRONIZATION, resource, false);
            }
          }
        }
      }
    }

    int queuedDeletions = 0;
    
    final Map<?, ? extends T> knownObjects = this.getKnownObjects();
    if (knownObjects == null) {

      for (final EventQueue<T> eventQueue : this.map.values()) {
        assert eventQueue != null;
        final Object key;
        final Event<? extends T> newestEvent;
        synchronized (eventQueue) {
          if (eventQueue.isEmpty()) {
            newestEvent = null;
            key = null;
            assert false : "eventQueue.isEmpty(): " + eventQueue;
          } else {
            key = eventQueue.getKey();
            if (key == null) {
              throw new IllegalStateException();
            }
            if (replacementKeys.contains(key)) {
              newestEvent = null;
            } else {
              // We have an EventQueue indexed under a key that
              // identifies a resource that no longer exists in
              // Kubernetes.  Inform any consumers via a deletion
              // event that this object was removed at some point from
              // Kubernetes.  The state of the object in question is
              // indeterminate.
              newestEvent = eventQueue.getLast();
              assert newestEvent != null;
            }
          }
        }
        if (newestEvent != null) {
          assert key != null;
          // We grab the last event in the queue in question and get
          // his resource; this will serve as the state of the
          // Kubernetes resource in question the last time we knew
          // about it.  This state is not necessarily, but could be,
          // the true actual last state of the resource in question.
          // The point is, the true state of the object when it was
          // deleted is unknown.  We build a new event to reflect all
          // this.
          //
          // Astute readers will realize that this could result in two
          // DELETION events enqueued, back to back, with identical
          // payloads.  See the deduplicate() method in EventQueue.
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
      this.initialPopulationCount = size + queuedDeletions;
    }
    
  }

  /**
   * Returns an {@link Object} which will be used as the key that will
   * uniquely identify the supplied {@code resource} to this {@link
   * EventQueueCollection}.
   *
   * <p>This method may return {@code null}, but only if {@code
   * resource} is {@code null} or is constructed in such a way that
   * its {@link HasMetadata#getMetadata()} method returns {@code
   * null}.</p>
   *
   * <p>Overrides of this method may return {@code null}, but only if
   * {@code resource} is {@code null}.
   *
   * <p>The default implementation of this method returns the return
   * value of the {@link HasMetadatas#getKey(HasMetadata)} method.</p>
   *
   * @param resource a {@link HasMetadata} for which a key should be
   * returned; may be {@code null} in which case {@code null} may be
   * returned
   *
   * @return a non-{@code null} key for the supplied {@code resource};
   * or {@code null} if {@code resource} is {@code null}
   *
   * @see HasMetadatas#getKey(HasMetadata)
   */
  protected Object getKey(final T resource) {
    return HasMetadatas.getKey(resource);
  }

  /**
   * Creates a new {@link EventQueue} suitable for holding {@link
   * Event}s {@linkplain Event#getKey() matching} the supplied {@code
   * key}.
   *
   * <p>This method never returns {@code null}.</p>
   *
   * <p>Overrides of this method must not return {@code null}.</p>
   *
   * @param the key {@linkplain EventQueue#getKey() for the new
   * <code>EventQueue</code>}; must not be {@code null}
   *
   * @return the new {@link EventQueue}; never {@code null}
   *
   * @exception NullPointerException if {@code key} is {@code null}
   *
   * @see EventQueue#EventQueue(Object)
   */
  protected EventQueue<T> createEventQueue(final Object key) {
    return new EventQueue<T>(key);
  }

  /**
   * Starts a new {@linkplain Thread#isDaemon() daemon} {@link Thread}
   * that, until {@link #close()} is called, removes {@link
   * EventQueue}s from this {@link EventQueueCollection} and supplies
   * them to the supplied {@link Consumer}.
   *
   * <p>Invoking this method does not block the calling {@link
   * Thread}.</p>
   *
   * @param siphon the {@link Consumer} that will process each {@link
   * EventQueue} as it becomes ready; must not be {@code null}
   *
   * @exception NullPointerException if {@code siphon} is {@code null}
   */
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

  /**
   * Semantically closes this {@link EventQueueCollection} by
   * detaching any {@link Consumer} previously attached via the {@link
   * #start(Consumer)} method.  {@linkplain #add(Object, Event.Type,
   * HasMetadata) Additions}, {@linkplain #replace(Collection, Object)
   * replacements} and {@linkplain #synchronize() synchronizations}
   * are still possible, but there won't be anything consuming any
   * events generated by or supplied to these operations.
   *
   * <p>A closed {@link EventQueueCollection} may be {@linkplain
   * #start(Consumer) started} again.</p>
   *
   * @see #start(Consumer)
   */
  @Override
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
            } catch (final TransientException transientException) {
              this.map.putIfAbsent(eventQueue.getKey(), eventQueue);
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
      if (this.initialPopulationCount > 0) {
        this.initialPopulationCount--;
      }
      Thread.interrupted(); // clear any interrupted status now that we know we're going to be successful
    }
    return returnValue;
  }

  /**
   * Creates an {@link Event} using the supplied raw materials and
   * returns it.
   *
   * <p>This method never returns {@code null}.</p>
   *
   * <p>Implementations of this method must not return {@code
   * null}.</p>
   *
   * <p>Implementations of this method must return a new {@link Event}
   * with every invocation.</p>
   *
   * @param source the {@linkplain Event#getSource() source} of the
   * {@link Event} that will be created; must not be null
   *
   * @param eventType the {@linkplain Event#getType() type} of {@link
   * Event} that will be created; must not be {@code null}
   *
   * @param resource the {@linkplain Event#getResource() resource} of
   * the {@link Event} that will be created; must not be
   * {@code null}
   */
  protected Event<T> createEvent(final Object source, final Event.Type eventType, final T resource) {
    Objects.requireNonNull(source);
    Objects.requireNonNull(eventType);
    Objects.requireNonNull(resource);
    return new Event<T>(source, eventType, null, resource);
  }

  /**
   * Adds a new {@link Event} constructed out of the parameters
   * supplied to this method to this {@link EventQueueCollection} and
   * returns the {@link Event} that was added.
   *
   * <p>This method may return {@code null}.</p>
   *
   * <p>This implementation {@linkplain #createEventQueue(Object)
   * creates an <code>EventQueue</code>} if necessary for the {@link
   * Event} that will be added, and then adds the new {@link Event} to
   * the queue.</p>
   *
   * @param source the {@linkplain Event#getSource() source} of the
   * {@link Event} that will be created and added; must not be null
   *
   * @param eventType the {@linkplain Event#getType() type} of {@link
   * Event} that will be created and added; must not be {@code null}
   *
   * @param resource the {@linkplain Event#getResource() resource} of
   * the {@link Event} that will be created and added; must not be
   * {@code null}
   *
   * @return the {@link Event} that was created and added, or {@code
   * null} if no {@link Event} was actually added as a result of this
   * method's invocation
   *
   * @exception NullPointerException if any of the parameters is
   * {@code null}
   *
   * @see Event
   */
  @Override
  public final Event<T> add(final Object source, final Event.Type eventType, final T resource) {
    return this.add(source, eventType, resource, true);
  }

  /**
   * Adds a new {@link Event} constructed out of the parameters
   * supplied to this method to this {@link EventQueueCollection} and
   * returns the {@link Event} that was added.
   *
   * <p>This method may return {@code null}.</p>
   *
   * <p>This implementation {@linkplain #createEventQueue(Object)
   * creates an <code>EventQueue</code>} if necessary for the {@link
   * Event} that will be added, and then adds the new {@link Event} to
   * the queue.</p>
   *
   * @param source the {@linkplain Event#getSource() source} of the
   * {@link Event} that will be created and added; must not be null
   *
   * @param eventType the {@linkplain Event#getType() type} of {@link
   * Event} that will be created and added; must not be {@code null}
   *
   * @param resource the {@linkplain Event#getResource() resource} of
   * the {@link Event} that will be created and added; must not be
   * {@code null}
   *
   * @param populate if {@link true} then this {@link
   * EventQueueCollection} will be internally marked as <em>initially
   * populated</em>
   *
   * @return the {@link Event} that was created and added, or {@code
   * null} if no {@link Event} was actually added as a result of this
   * method's invocation
   *
   * @exception NullPointerException if any of the parameters is
   * {@code null}
   *
   * @see Event
   */
  private final Event<T> add(final Object source, final Event.Type eventType, final T resource, final boolean populate) {
    final Event<T> event = this.createEvent(source, eventType, resource);
    if (event == null) {
      throw new IllegalStateException("createEvent() == null");
    }
    return this.add(event, populate);
  }

  /**
   * Adds the supplied {@link Event} to this {@link
   * EventQueueCollection} and returns the {@link Event} that was
   * added.
   *
   * <p>This method may return {@code null}.</p>
   *
   * <p>This implementation {@linkplain #createEventQueue(Object)
   * creates an <code>EventQueue</code>} if necessary for the {@link
   * Event} that will be added, and then adds the new {@link Event} to
   * the queue.</p>
   *
   * @param event the {@link Event} to add; must not be {@code null}
   *
   * @param populate if {@link true} then this {@link
   * EventQueueCollection} will be internally marked as <em>initially
   * populated</em>
   *
   * @return the {@link Event} that was created and added, or {@code
   * null} if no {@link Event} was actually added as a result of this
   * method's invocation
   *
   * @exception NullPointerException if any of the parameters is
   * {@code null}
   *
   * @see Event
   */
  private final Event<T> add(final Event<T> event, final boolean populate) {
    Objects.requireNonNull(event);
    final Object key = event.getKey();
    if (key == null) {
      throw new IllegalArgumentException("event.getKey() == null");
    }
    
    Event<T> returnValue = null;
    EventQueue<T> eventQueue;
    synchronized (this) {
      if (populate) {
        this.populated = true;
      }
      eventQueue = this.map.get(key);
      final boolean eventQueueExisted = eventQueue != null;
      if (eventQueue == null) {
        eventQueue = this.createEventQueue(key);
        if (eventQueue == null) {
          throw new IllegalStateException("createEventQueue(key) == null: " + key);
        }
      }
      synchronized (eventQueue) {
        if (eventQueue.addEvent(event)) {
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


  /*
   * Inner and nested classes.
   */
  

  /**
   * A {@link RuntimeException} indicating that a {@link Consumer}
   * {@linkplain EventQueueCollection#start(Consumer) started} by an
   * {@link EventQueueCollection} has encountered an error that might
   * not happen if the consumption operation is retried.
   *
   * @author <a href="https://about.me/lairdnelson"
   * target="_parent">Laird Nelson</a>
   *
   * @see EventQueueCollection
   */
  public static final class TransientException extends RuntimeException {


    /*
     * Static fields.
     */


    /**
     * The version of this class for {@linkplain Serializable
     * serialization purposes}.
     *
     * @see Serializable
     */
    private static final long serialVersionUID = 1L;


    /*
     * Constructors.
     */


    /**
     * Creates a new {@link TransientException}.
     */
    public TransientException() {
      super();
    }

    /**
     * Creates a new {@link TransientException}.
     *
     * @param message a detail message describing the error; may be
     * {@code null}
     */
    public TransientException(final String message) {
      super(message);
    }

    /**
     * Creates a new {@link TransientException}.
     *
     * @param cause the {@link Throwable} that caused this {@link
     * TransientException} to be created; may be {@code null}
     */
    public TransientException(final Throwable cause) {
      super(cause);
    }

    /**
     * Creates a new {@link TransientException}.
     *
     * @param message a detail message describing the error; may be
     * {@code null}
     *
     * @param cause the {@link Throwable} that caused this {@link
     * TransientException} to be created; may be {@code null}
     */
    public TransientException(final String message, final Throwable cause) {
      super(message, cause);
    }
    
  }

}
