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

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException; // for javadoc only
import java.util.Objects;

import java.util.function.Consumer;

import java.util.logging.Level;
import java.util.logging.Logger;

import io.fabric8.kubernetes.api.model.HasMetadata;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

/**
 * A publicly-unmodifiable {@link AbstractCollection} of {@link
 * AbstractEvent}s produced by an {@link EventQueueCollection}.
 *
 * <p>All {@link AbstractEvent}s in an {@link EventQueue} describe the
 * life of a single {@linkplain HasMetadata resource} in
 * Kubernetes.</p>
 *
 * <h2>Thread Safety</h2>
 *
 * <p>This class is safe for concurrent use by multiple {@link
 * Thread}s.  Some operations, like the usage of the {@link
 * #iterator()} method, require that callers synchronize on the {@link
 * EventQueue} directly.  This class' internals synchronize on {@code
 * this} when locking is needed.</p>
 *
 * <p>Overrides of this class must also be safe for concurrent use by
 * multiple {@link Thread}s.</p>
 *
 * @param <T> the type of a Kubernetes resource
 *
 * @author <a href="https://about.me/lairdnelson"
 * target="_parent">Laird Nelson</a>
 *
 * @see EventQueueCollection
 */
@ThreadSafe
public class EventQueue<T extends HasMetadata> extends AbstractCollection<AbstractEvent<T>> {

  
  /*
   * Instance fields.
   */


  /**
   * A {@link Logger} for use by this {@link EventQueue}.
   *
   * <p>This field is never {@code null}.</p>
   *
   * @see #createLogger()
   */
  protected final Logger logger;

  /**
   * The key identifying the Kubernetes resource to which all of the
   * {@link AbstractEvent}s managed by this {@link EventQueue} apply.
   *
   * <p>This field is never {@code null}.</p>
   */
  private final Object key;

  /**
   * The actual underlying queue of {@link AbstractEvent}s.
   *
   * <p>This field is never {@code null}.</p>
   */
  @GuardedBy("this")
  private final LinkedList<AbstractEvent<T>> events;


  /*
   * Constructors.
   */


  /**
   * Creates a new {@link EventQueue}.
   *
   * @param key the key identifying the Kubernetes resource to which
   * all of the {@link AbstractEvent}s managed by this {@link
   * EventQueue} apply; must not be {@code null}
   *
   * @exception NullPointerException if {@code key} is {@code null}
   *
   * @exception IllegalStateException if the {@link #createLogger()}
   * method returns {@code null}
   */
  protected EventQueue(final Object key) {
    super();
    this.logger = this.createLogger();
    if (this.logger == null) {
      throw new IllegalStateException("createLogger() == null");
    }
    final String cn = this.getClass().getName();
    final String mn = "<init>";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn, key);
    }
    this.key = Objects.requireNonNull(key);
    this.events = new LinkedList<>();
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn);
    }
  }


  /*
   * Instance methods.
   */


  /**
   * Returns a {@link Logger} for use by this {@link EventQueue}.
   *
   * <p>This method never returns {@code null}.</p>
   *
   * <p>Overrides of this method must not return {@code null}.</p>
   *
   * @return a non-{@code null} {@link Logger}
   */
  protected Logger createLogger() {
    return Logger.getLogger(this.getClass().getName());
  }

  /**
   * Returns the key identifying the Kubernetes resource to which all
   * of the {@link AbstractEvent}s managed by this {@link EventQueue}
   * apply.
   *
   * <p>This method never returns {@code null}.</p>
   *
   * @return a non-{@code null} {@link Object}
   *
   * @see #EventQueue(Object)
   */
  public final Object getKey() {
    final String cn = this.getClass().getName();
    final String mn = "getKey";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn);
    }
    final Object returnValue = this.key;
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn, returnValue);
    }
    return returnValue;
  }

  /**
   * Returns {@code true} if this {@link EventQueue} is empty.
   *
   * @return {@code true} if this {@link EventQueue} is empty; {@code
   * false} otherwise
   *
   * @see #size()
   */
  public synchronized final boolean isEmpty() {
    final String cn = this.getClass().getName();
    final String mn = "isEmpty";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn);
    }
    final boolean returnValue = this.events.isEmpty();
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn, Boolean.valueOf(returnValue));
    }
    return returnValue;
  }

  /**
   * Returns the size of this {@link EventQueue}.
   *
   * <p>This method never returns an {@code int} less than {@code
   * 0}.</p>
   *
   * @return the size of this {@link EventQueue}; never negative
   *
   * @see #isEmpty()
   */
  @Override
  public synchronized final int size() {
    final String cn = this.getClass().getName();
    final String mn = "size";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn);
    }
    final int returnValue = this.events.size();
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn, Integer.valueOf(returnValue));
    }
    return returnValue;
  }

  /**
   * Adds the supplied {@link AbstractEvent} to this {@link
   * EventQueue} under certain conditions.
   *
   * <p>The supplied {@link AbstractEvent} is added to this {@link
   * EventQueue} if:</p>
   *
   * <ul>
   *
   * <li>its {@linkplain AbstractEvent#getKey() key} is equal to this
   * {@link EventQueue}'s {@linkplain #getKey() key}</li>
   *
   * <li>it is either not a {@linkplain SynchronizationEvent}
   * synchronization event}, or it <em>is</em> a {@linkplain
   * SynchronizationEvent synchronization event} and this {@link
   * EventQueue} does not represent a sequence of events that
   * {@linkplain #resultsInDeletion() describes a deletion}, and</li>
   *
   * <li>optional {@linkplain #compress(Collection) compression} does
   * not result in this {@link EventQueue} being empty</li>
   *
   * </ul>
   *
   * @param event the {@link AbstractEvent} to add; must not be {@code
   * null}
   *
   * @return {@code true} if an addition took place and {@linkplain
   * #compress(Collection) optional compression} did not result in
   * this {@link EventQueue} {@linkplain #isEmpty() becoming empty};
   * {@code false} otherwise
   *
   * @exception NullPointerException if {@code event} is {@code null}
   *
   * @exception IllegalArgumentException if {@code event}'s
   * {@linkplain AbstractEvent#getKey() key} is not equal to this
   * {@link EventQueue}'s {@linkplain #getKey() key}
   *
   * @see #compress(Collection)
   *
   * @see SynchronizationEvent
   *
   * @see #resultsInDeletion()
   */
  final boolean addEvent(final AbstractEvent<T> event) {
    final String cn = this.getClass().getName();
    final String mn = "addEvent";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn, event);
    }
    
    Objects.requireNonNull(event);

    final Object key = this.getKey();
    if (!key.equals(event.getKey())) {
      throw new IllegalArgumentException("!this.getKey().equals(event.getKey()): " + key + ", " + event.getKey());
    }

    boolean returnValue = false;

    final AbstractEvent.Type eventType = event.getType();
    assert eventType != null;

    synchronized (this) {
      if (!(event instanceof SynchronizationEvent) || !this.resultsInDeletion()) {
        // If the event is NOT a synchronization event (so it's an
        // addition, modification, or deletion)...
        // ...OR if it IS a synchronization event AND we are NOT
        // already going to delete this queue...
        returnValue = this.events.add(event);
        if (returnValue) {
          this.deduplicate();
          final Collection<AbstractEvent<T>> readOnlyEvents = Collections.unmodifiableCollection(this.events);
          final Collection<AbstractEvent<T>> newEvents = this.compress(readOnlyEvents);
          if (newEvents != readOnlyEvents) {
            this.events.clear();
            if (newEvents != null && !newEvents.isEmpty()) {
              this.events.addAll(newEvents);
            }
          }
          returnValue = !this.isEmpty();
        }
      }
    }
    
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn, Boolean.valueOf(returnValue));
    }
    return returnValue;
  }

  /**
   * Returns the last (and definitionally newest) {@link
   * AbstractEvent} in this {@link EventQueue}.
   *
   * <p>This method never returns {@code null}.</p>
   *
   * @return the last {@link AbstractEvent} in this {@link
   * EventQueue}; never {@code null}
   *
   * @exception NoSuchElementException if this {@link EventQueue} is
   * {@linkplain #isEmpty() empty}
   */
  synchronized final AbstractEvent<T> getLast() {
    final String cn = this.getClass().getName();
    final String mn = "getLast";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn);
    }
    final AbstractEvent<T> returnValue = this.events.getLast();
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn, returnValue);
    }
    return returnValue;
  }

  /**
   * Synchronizes on this {@link EventQueue} and, while holding its
   * monitor, invokes the {@link Consumer#accept(Object)} method on
   * the supplied {@link Consumer} for every {@link AbstractEvent} in
   * this {@link EventQueue}.
   *
   * @param action the {@link Consumer} in question; must not be
   * {@code null}
   *
   * @exception NullPointerException if {@code action} is {@code null}
   */
  @Override
  public synchronized final void forEach(final Consumer<? super AbstractEvent<T>> action) {
    super.forEach(action);
  }
  
  /**
   * Synchronizes on this {@link EventQueue} and, while holding its
   * monitor, returns an unmodifiable {@link Iterator} over its
   * contents.
   *
   * <p>This method never returns {@code null}.</p>
   *
   * @return a non-{@code null} unmodifiable {@link Iterator} of
   * {@link AbstractEvent}s
   */
  @Override
  public synchronized final Iterator<AbstractEvent<T>> iterator() {
    return Collections.unmodifiableCollection(this.events).iterator();
  }

  /**
   * If this {@link EventQueue}'s {@linkplain #size() size} is greater
   * than {@code 2}, and if its last two {@link AbstractEvent}s are
   * {@linkplain AbstractEvent.Type#DELETION deletions}, and if the
   * next-to-last deletion {@link AbstractEvent}'s {@linkplain
   * AbstractEvent#isFinalStateKnown() state is known}, then this method
   * causes that {@link AbstractEvent} to replace the two under consideration.
   *
   * <p>This method is called only by the {@link #addEvent(AbstractEvent)}
   * method.</p>
   *
   * @see #addEvent(AbstractEvent)
   */
  private synchronized final void deduplicate() {
    final String cn = this.getClass().getName();
    final String mn = "deduplicate";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn);
    }
    final int size = this.size();
    if (size > 2) {
      final AbstractEvent<T> lastEvent = this.events.get(size - 1);
      final AbstractEvent<T> nextToLastEvent = this.events.get(size - 2);
      final AbstractEvent<T> event;
      if (lastEvent != null && nextToLastEvent != null && AbstractEvent.Type.DELETION.equals(lastEvent.getType()) && AbstractEvent.Type.DELETION.equals(nextToLastEvent.getType())) {
        event = nextToLastEvent.isFinalStateKnown() ? nextToLastEvent : lastEvent;
      } else {
        event = null;
      }
      if (event != null) {
        this.events.set(size - 2, event);
        this.events.remove(size - 1);
      }
    }
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn);
    }
  }

  /**
   * Returns {@code true} if this {@link EventQueue} is {@linkplain
   * #isEmpty() not empty} and the {@linkplain #getLast() last
   * <code>AbstractEvent</code> in this <code>EventQueue</code>} is a
   * {@linkplain AbstractEvent.Type#DELETION deletion event}.
   *
   * @return {@code true} if this {@link EventQueue} currently
   * logically represents the deletion of a resource, {@code false}
   * otherwise
   */
  synchronized final boolean resultsInDeletion() {
    final String cn = this.getClass().getName();
    final String mn = "resultsInDeletion";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn);
    }
    final boolean returnValue = !this.isEmpty() && this.getLast().getType().equals(AbstractEvent.Type.DELETION);
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn, Boolean.valueOf(returnValue));
    }
    return returnValue;
  }

  /**
   * Performs a compression operation on the supplied {@link
   * Collection} of {@link AbstractEvent}s and returns the result of that
   * operation.
   *
   * <p>This method may return {@code null}, which will result in the
   * emptying of this {@link EventQueue}.</p>
   *
   * <p>This method is called while holding this {@link EventQueue}'s
   * monitor.</p>
   *
   * <p>This method is called when an {@link EventQueueCollection} (or
   * some other {@link AbstractEvent} producer with access to
   * package-protected methods of this class) adds an {@link AbstractEvent} to
   * this {@link EventQueue} and provides the {@link EventQueue}
   * implementation with the ability to eliminate duplicates or
   * otherwise compress the event stream it represents.</p>
   *
   * <p>This implementation simply returns the supplied {@code events}
   * {@link Collection}; i.e. no compression is performed.</p>
   *
   * @param events an {@link
   * Collections#unmodifiableCollection(Collection) unmodifiable
   * <tt>Collection</tt>} of {@link AbstractEvent}s representing the
   * current state of this {@link EventQueue}; will never be {@code
   * null}
   *
   * @return the new state that this {@link EventQueue} should assume;
   * may be {@code null}; may simply be the supplied {@code events}
   * {@link Collection} if compression is not desired or implemented
   */
  protected Collection<AbstractEvent<T>> compress(final Collection<AbstractEvent<T>> events) {
    return events;
  }

  /**
   * Returns a hashcode for this {@link EventQueue}.
   *
   * @return a hashcode for this {@link EventQueue}
   *
   * @see #equals(Object)
   */
  @Override
  public final int hashCode() {
    int hashCode = 17;

    Object value = this.getKey();
    int c = value == null ? 0 : value.hashCode();
    hashCode = 37 * hashCode + c;

    synchronized (this) {
      value = this.events;
      c = value == null ? 0 : value.hashCode();
    }
    hashCode = 37 * hashCode + c;

    return hashCode;
  }

  /**
   * Returns {@code true} if the supplied {@link Object} is also an
   * {@link EventQueue} and is equal in all respects to this one.
   *
   * @param other the {@link Object} to test; may be {@code null} in
   * which case {@code null} will be returned
   *
   * @return {@code true} if the supplied {@link Object} is also an
   * {@link EventQueue} and is equal in all respects to this one;
   * {@code false} otherwise
   *
   * @see #hashCode()
   */
  @Override
  public final boolean equals(final Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof EventQueue) {
      final EventQueue<?> her = (EventQueue<?>)other;

      final Object key = this.getKey();
      if (key == null) {
        if (her.getKey() != null) {
          return false;
        }
      } else if (!key.equals(her.getKey())) {
        return false;
      }

      synchronized (this) {
        final Object events = this.events;
        if (events == null) {
          synchronized (her) {
            if (her.events != null) {
              return false;
            }
          }
        } else {
          synchronized (her) {
            if (!events.equals(her.events)) {
              return false;
            }
          }
        }
      }

      return true;
    } else {
      return false;
    }
  }

  /**
   * Returns a {@link String} representation of this {@link
   * EventQueue}.
   *
   * <p>This method never returns {@code null}.</p>
   *
   * @return a non-{@code null} {@link String} representation of this
   * {@link EventQueue}
   */
  @Override
  public synchronized final String toString() {
    return new StringBuilder().append(this.getKey()).append(": ").append(this.events).toString();
  }

}
