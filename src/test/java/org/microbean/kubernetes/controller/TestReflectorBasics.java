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

import java.time.Duration;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import java.util.function.Consumer;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.Pod;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;

import io.fabric8.kubernetes.client.dsl.Listable;
import io.fabric8.kubernetes.client.dsl.VersionWatchable;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

import io.fabric8.kubernetes.client.Watcher;

public class TestReflectorBasics {

  public TestReflectorBasics() {
    super();
  }

  @Test
  public void testBasics() throws Exception {
    assumeFalse(Boolean.getBoolean("skipClusterTests"));

    final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
    assertNotNull(executorService);

    final DefaultKubernetesClient client = new DefaultKubernetesClient();

    // We'll use this as our "known objects".
    final Map<Object, ConfigMap> configMaps = new HashMap<>();

    // Create a new EventCache implementation that "knows about" our
    // known objects.
    final EventQueueCollection<ConfigMap> eventQueues = new EventQueueCollection<>(configMaps, 16, 0.75f);

    // Create a consumer that can remove and process EventQueues from
    // our EventCache implementation.  It will also update our "known
    // objects".
    final Consumer<? super EventQueue<? extends ConfigMap>> siphon = (q) -> {
      assertNotNull(q);
      assertFalse(q.isEmpty());
      for (final Event<? extends ConfigMap> event : q) {
        assertNotNull(event);
        System.out.println("*** received event: " + event);
        final Event.Type type = event.getType();
        assertNotNull(type);
        switch (type) {
        case DELETION:
          configMaps.remove(event.getKey());
          break;
        default:
          configMaps.put(event.getKey(), event.getResource());
          break;
        }
      }
    };

    // Begin sucking EventQueue instances out of the cache on a
    // separate Thread.  Obviously there aren't any yet.  This creates
    // a new (daemon) Thread and starts it.  It will block
    // immediately, waiting for new EventQueues to show up in our
    // EventQueueCollection.
    eventQueues.start(siphon);

    // Now create a Reflector that we'll then hook up to Kubernetes
    // and instruct to "reflect" its events "into" our
    // EventQueueCollection, thus making EventQueues available to the
    // Consumer we built above.
    final Reflector<ConfigMap> reflector =
      new Reflector<ConfigMap>(client.configMaps(),
                               eventQueues,
                               executorService,
                               Duration.ofSeconds(10));

    // Start the reflection process: this effectively puts EventQueue
    // instances into the cache.  This creates a new (daemon) Thread
    // and starts it.
    reflector.start();

    // Sleep for a bit on the main thread so you can see what's going
    // on and try adding some resources to Kubernetes in a terminal
    // window.  Watch as the consumer we built above will report on
    // all the additions, updates, deletions and synchronizations.
    Thread.sleep(1L * 60L * 1000L);

    // Shut down production of events (notably before we shut down
    // consumption of events).
    reflector.close();
    client.close();

    // Shut down reception of events (notably after we shut down
    // production).
    eventQueues.close();
  }

}
