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

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import org.junit.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeFalse;

public class TestReflectorBasics {

  public TestReflectorBasics() {
    super();
  }

  @Test
  public void testBasics() throws Exception {
    assumeFalse(Boolean.getBoolean("skipClusterTests"));

    // We'll use this as our "known objects".
    final Map<Object, ConfigMap> configMaps = new HashMap<>();

    // Create a new EventCache implementation that "knows about" our
    // known objects.
    final EventQueueCollection<ConfigMap> eventQueues = new EventQueueCollection<>(configMaps, 16, 0.75f);

    // Create a consumer that can remove and process EventQueues from
    // our EventCache implementation.  It will also update our "known
    // objects" as necessary.
    final Consumer<? super EventQueue<? extends ConfigMap>> siphon =
      new ResourceTrackingEventQueueConsumer<ConfigMap>(configMaps) {
        @Override
        protected final void accept(final AbstractEvent<? extends ConfigMap> event) {
          assertNotNull(event);
          System.out.println("*** received event: " + event);
        }
      };

    // Begin sucking EventQueue instances out of the cache on a
    // separate Thread.  Obviously there aren't any yet.  This creates
    // a new (daemon) Thread and starts it.  It will block
    // immediately, waiting for new EventQueues to show up in our
    // EventQueueCollection.
    eventQueues.start(siphon);

    // Connect to Kubernetes using a combination of system properties,
    // environment variables and ~/.kube/config settings as detailed
    // here:
    // https://github.com/fabric8io/kubernetes-client/blob/v3.2.0/README.md#configuring-the-client.
    // We'll use this client when we create a Reflector below.
    final DefaultKubernetesClient client = new DefaultKubernetesClient();

    // Now create a Reflector that we'll then hook up to Kubernetes
    // and instruct to "reflect" its events "into" our
    // EventQueueCollection, thus making EventQueues available to the
    // Consumer we built above.
    final Reflector<ConfigMap> reflector =
      new Reflector<ConfigMap>(client.configMaps(),
                               eventQueues,
                               Duration.ofSeconds(10));

    // Start the reflection process: this effectively puts EventQueue
    // instances into the cache.  This creates a new (daemon) Thread
    // and starts it.
    System.out.println("*** starting reflector");
    reflector.start();

    // Sleep for a bit on the main thread so you can see what's going
    // on and try adding some resources to Kubernetes in a terminal
    // window.  Watch as the consumer we built above will report on
    // all the additions, updates, deletions and synchronizations.
    Thread.sleep(1L * 60L * 1000L);

    // Close the Reflector.  This cancels any scheduled
    // synchronization tasks.
    System.out.println("*** closing reflector");
    reflector.close();
    
    // Close the client, now that no one will be calling it anymore.
    System.out.println("*** closing client");
    client.close();

    // Shut down reception of events now that no one is making any
    // more of them.
    System.out.println("*** closing eventQueues");
    eventQueues.close();
  }

}
