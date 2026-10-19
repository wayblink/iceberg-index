/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.index.vector;

/**
 * Makes the kernel a definition asks for.
 *
 * <p>A kernel is made per definition rather than kept as a singleton, because dimension, metric and
 * the options are part of what it does. What does not depend on the definition — the name and what
 * the kernel can build — belongs to the factory, so that a request can be refused before anything
 * is made.
 */
public interface VectorKernelFactory {

  String name();

  KernelCapabilities capabilities();

  VectorKernel create(VectorIndexDefinition definition);
}
