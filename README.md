# microBean Kubernetes Controller

[![Build Status](https://travis-ci.org/microbean/microbean-kubernetes-controller.svg?branch=master)](https://travis-ci.org/microbean/microbean-kubernetes-controller)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.microbean/microbean-kubernetes-controller/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.microbean/microbean-kubernetes-controller)

The `microbean-kubernetes-controller` project contains an idiomatic
Java implementation of the [Kubernetes controller
framework][tools-cache].  This lets you write
_controllers_&mdash;sometimes known as _operators_&mdash;in Java,
using the [fabric8 Kubernetes client][kubernetes-client].

There is a [hopefully useful eleven-part series of blog posts on the
subject][blog].

[kubernetes-client]: https://github.com/fabric8io/kubernetes-client/blob/master/README.md
[tools-cache]: https://github.com/kubernetes/kubernetes/blob/v1.9.0/staging/src/k8s.io/client-go/tools/cache/
[blog]: https://lairdnelson.wordpress.com/2018/01/07/understanding-kubernetes-tools-cache-package-part-0/
