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

import java.util.Map;
import java.util.Objects;

import java.util.function.Consumer;

import java.util.logging.Level;
import java.util.logging.Logger;

import io.fabric8.kubernetes.api.model.HasMetadata;

import net.jcip.annotations.GuardedBy;

/**
 * A {@link Consumer} of {@link EventQueue}s that tracks the
 * Kubernetes resources they contain before allowing subclasses to
 * process their individual {@link Event}s.
 *
 * <p>Typically you would supply an implementation of this class to a
 * {@link Controller}.</p>
 *
 * @param <T> a Kubernetes resource type
 *
 * @author <a href="https://about.me/lairdnelson"
 * target="_parent">Laird Nelson</a>
 *
 * @see #accept(AbstractEvent)
 *
 * @see Controller
 */
public abstract class ResourceTrackingEventQueueConsumer<T extends HasMetadata> implements Consumer<EventQueue<? extends T>> {


  /*
   * Instance fields.
   */


  /**
   * A mutable {@link Map} of {@link HasMetadata} objects indexed by
   * their keys (often a pairing of namespace and name).
   *
   * <p>This field may be {@code null} in which case no resource
   * tracking will take place.</p>
   *
   * <p>The value of this field is {@linkplain
   * #ResourceTrackingEventQueueConsumer(Map) supplied at construction
   * time} and is <strong>synchronized on</strong> and written to, if
   * non-{@code null}, by the {@link #accept(EventQueue)} method.</p>
   *
   * <p>This class <strong>synchronizes on this field's
   * value</strong>, if it is non-{@code null}, when mutating its
   * contents.</p>
   */
  @GuardedBy("itself")
  private final Map<Object, T> knownObjects;

  /**
   * A {@link Logger} for use by this {@link
   * ResourceTrackingEventQueueConsumer} implementation.
   *
   * <p>This field is never {@code null}.</p>
   *
   * @see #createLogger()
   */
  protected final Logger logger;


  /*
   * Constructors.
   */


  /**
   * Creates a new {@link ResourceTrackingEventQueueConsumer}.
   *
   * @param knownObjects a mutable {@link Map} of {@link HasMetadata}
   * objects indexed by their keys (often a pairing of namespace and
   * name); may be {@code null} if deletion tracking is not needed;
   * <strong>will have its contents changed</strong> by this {@link
   * ResourceTrackingEventQueueConsumer}'s {@link #accept(EventQueue)}
   * method; <strong>will be synchronized on</strong> by this {@link
   * ResourceTrackingEventQueueConsumer}'s {@link #accept(EventQueue)}
   * method
   *
   * @see #accept(EventQueue)
   */
  protected ResourceTrackingEventQueueConsumer(final Map<Object, T> knownObjects) {
    super();
    this.logger = this.createLogger();
    if (this.logger == null) {
      throw new IllegalStateException("createLogger() == null");
    }
    final String cn = this.getClass().getName();
    final String mn = "<init>";
    if (this.logger.isLoggable(Level.FINER)) {
      final String knownObjectsString;
      if (knownObjects == null) {
        knownObjectsString = null;
      } else {
        synchronized (knownObjects) {
          knownObjectsString = knownObjects.toString();
        }
      }
      this.logger.entering(cn, mn, knownObjectsString);
    }
    this.knownObjects = knownObjects;
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn);
    }
  }


  /*
   * Instance methods.
   */


  /**
   * Returns a {@link Logger} for use with this {@link
   * ResourceTrackingEventQueueConsumer}.
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
   * {@linkplain EventQueue#iterator() Loops through} all the {@link
   * AbstractEvent}s in the supplied {@link EventQueue}, keeping track
   * of the {@link HasMetadata} it concerns along the way by
   * <strong>synchronizing on</strong> and writing to the {@link Map}
   * {@linkplain #ResourceTrackingEventQueueConsumer(Map) supplied at
   * construction time}.
   *
   * <p>Individual {@link AbstractEvent}s are forwarded on to the
   * {@link #accept(AbstractEvent)} method.</p>
   *
   * <h2>Implementation Notes</h2>
   *
   * <p>This loosely models the <a
   * href="https://github.com/kubernetes/client-go/blob/v6.0.0/tools/cache/shared_informer.go#L343">{@code
   * HandleDeltas} function in {@code
   * tools/cache/shared_informer.go}</a>.  The final distribution step
   * is left unimplemented on purpose.</p>
   *
   * @param eventQueue the {@link EventQueue} to process; may be
   * {@code null} in which case no action will be taken
   *
   * @see #accept(AbstractEvent)
   */
  @Override
  public final void accept(final EventQueue<? extends T> eventQueue) {
    final String cn = this.getClass().getName();
    final String mn = "accept";
    if (eventQueue == null) {
      if (this.logger.isLoggable(Level.FINER)) {
        this.logger.entering(cn, mn, null);
      }
    } else {
      synchronized (eventQueue) {
        if (this.logger.isLoggable(Level.FINER)) {
          this.logger.entering(cn, mn, eventQueue);
        }

        final Object key = eventQueue.getKey();
        if (key == null) {
          throw new IllegalStateException("eventQueue.getKey() == null; eventQueue: " + eventQueue);
        }

        for (final AbstractEvent<? extends T> event : eventQueue) {
          if (event != null) {

            assert key.equals(event.getKey());

            final Event.Type eventType = event.getType();
            assert eventType != null;

            final T newResource = event.getResource();

            if (event.getPriorResource() != null && this.logger.isLoggable(Level.FINE)) {
              this.logger.logp(Level.FINE, cn, mn, "Unexpected state; event has a priorResource: {0}", event.getPriorResource());
            }

            final T priorResource;
            final AbstractEvent<? extends T> newEvent;

            if (this.knownObjects == null) {
              priorResource = null;
              newEvent = event;
            } else if (Event.Type.DELETION.equals(eventType)) {

              // "Forget" (untrack) the object in question.
              synchronized (this.knownObjects) {
                priorResource = this.knownObjects.remove(key);
              }

              newEvent = event;
            } else {
              assert eventType.equals(Event.Type.ADDITION) || eventType.equals(Event.Type.MODIFICATION);

              // "Learn" (track) the resource in question.
              synchronized (this.knownObjects) {
                priorResource = this.knownObjects.put(key, newResource);
              }

              if (event instanceof SynchronizationEvent) {
                if (priorResource == null) {
                  assert Event.Type.ADDITION.equals(eventType) : "!Event.Type.ADDITION.equals(eventType): " + eventType;
                  newEvent = event;
                } else {
                  assert Event.Type.MODIFICATION.equals(eventType) : "!Event.Type.MODIFICATION.equals(eventType): " + eventType;
                  newEvent = this.createSynchronizationEvent(Event.Type.MODIFICATION, priorResource, newResource);
                }
              } else if (priorResource == null) {
                if (Event.Type.ADDITION.equals(eventType)) {
                  newEvent = event;
                } else {
                  newEvent = this.createEvent(Event.Type.ADDITION, null, newResource);
                }
              } else {
                newEvent = this.createEvent(Event.Type.MODIFICATION, priorResource, newResource);
              }
            }

            assert newEvent != null;
            assert newEvent instanceof SynchronizationEvent || newEvent instanceof Event;

            // This is the final consumption/distribution step; it is
            // an abstract method in this class.
            this.accept(newEvent);

          }
        }

      }
    }
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn);
    }
  }

  /**
   * Creates and returns a new {@link Event}.
   *
   * <p>This method never returns {@code null}.</p>
   *
   * <p>Overrides of this method must not return {@code null}.</p>
   *
   * @param eventType the {@link AbstractEvent.Type} for the new
   * {@link Event}; must not be {@code null}; when supplied by the
   * {@link #accept(EventQueue)} method's internals, will always be
   * either {@link AbstractEvent.Type#ADDITION} or {@link
   * AbstractEvent.Type#MODIFICATION}
   *
   * @param priorResource the prior state of the resource the new
   * {@link Event} will represent; may be (and often is) {@code null}
   *
   * @param resource the latest state of the resource the new {@link
   * Event} will represent; must not be {@code null}
   *
   * @return a new, non-{@code null} {@link Event} with each
   * invocation
   *
   * @exception NullPointerException if {@code eventType} or {@code
   * resource} is {@code null}
   */
  protected Event<T> createEvent(final Event.Type eventType, final T priorResource, final T resource) {
    final String cn = this.getClass().getName();
    final String mn = "createEvent";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn, new Object[] { eventType, priorResource, resource });
    }
    Objects.requireNonNull(eventType);
    final Event<T> returnValue = new Event<>(this, eventType, priorResource, resource);
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn, returnValue);
    }
    return returnValue;
  }

  /**
   * Creates and returns a new {@link SynchronizationEvent}.
   *
   * <p>This method never returns {@code null}.</p>
   *
   * <p>Overrides of this method must not return {@code null}.</p>
   *
   * @param eventType the {@link AbstractEvent.Type} for the new
   * {@link SynchronizationEvent}; must not be {@code null}; when
   * supplied by the {@link #accept(EventQueue)} method's internals,
   * will always be {@link AbstractEvent.Type#MODIFICATION}
   *
   * @param priorResource the prior state of the resource the new
   * {@link SynchronizationEvent} will represent; may be (and often
   * is) {@code null}
   *
   * @param resource the latest state of the resource the new {@link
   * SynchronizationEvent} will represent; must not be {@code null}
   *
   * @return a new, non-{@code null} {@link SynchronizationEvent} with
   * each invocation
   *
   * @exception NullPointerException if {@code eventType} or {@code
   * resource} is {@code null}
   */
  protected SynchronizationEvent<T> createSynchronizationEvent(final Event.Type eventType, final T priorResource, final T resource) {
    final String cn = this.getClass().getName();
    final String mn = "createSynchronizationEvent";
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.entering(cn, mn, new Object[] { eventType, priorResource, resource });
    }
    Objects.requireNonNull(eventType);
    final SynchronizationEvent<T> returnValue = new SynchronizationEvent<>(this, eventType, priorResource, resource);
    if (this.logger.isLoggable(Level.FINER)) {
      this.logger.exiting(cn, mn, returnValue);
    }
    return returnValue;
  }

  /**
   * Called to process a given {@link AbstractEvent} from the {@link
   * EventQueue} supplied to the {@link #accept(EventQueue)} method,
   * <strong>with that {@link EventQueue}'s monitor held</strong>.
   *
   * <p>Implementations of this method should be relatively fast as
   * this method dictates the speed of {@link EventQueue}
   * processing.</p>
   *
   * @param event the {@link AbstractEvent} encountered in the {@link
   * EventQueue}; must not be {@code null}
   *
   * @exception NullPointerException if {@code event} is {@code null}
   *
   * @see #accept(EventQueue)
   */
  protected abstract void accept(final AbstractEvent<? extends T> event);

}
