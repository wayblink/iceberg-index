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
package org.apache.iceberg.index;

/**
 * The pointer to the registry file that lists a table's indexes.
 *
 * <p>A registry file is immutable once written, so a registration never rewrites it in place: a
 * writer writes a new file and then moves the pointer to it. Moving the pointer is the only
 * mutation, and it must be conditional, or two writers registering different indexes at the same
 * time would each move it to a registry that does not list the other's index.
 *
 * <p>Where the pointer lives decides how strong that condition is. A table property is compared and
 * swapped by the catalog on commit, so a lost race throws and the loser retries against the
 * winner's registry.
 */
interface IndexRegistryPointer {

  /**
   * Returns the registry file the table currently points to.
   *
   * @return the file name, or null when the table points at nothing yet and the registry is the
   *     single legacy file
   */
  String current();

  /**
   * Points the table at a registry file, unless someone else moved the pointer first.
   *
   * @param expected the file name read before writing the new one, or null when there was none
   * @param next the file name to point at
   * @return true if the pointer now names {@code next}, false if it was moved by someone else
   */
  boolean compareAndSet(String expected, String next);
}
