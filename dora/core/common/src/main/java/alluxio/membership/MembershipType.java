/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.membership;

/**
 * MembershipManager type.
 */
public enum MembershipType {
  STATIC, // Use a static file to configure a static member list for MembershipManager
  ETCD, // Use etcd for MembershipManager
  NOOP // For regression purpose, still leverage Master for worker registration
}
