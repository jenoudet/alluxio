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

package alluxio.master.services;

/**
 * Interface defining the behavior of services to be registered with the
 * {@link alluxio.master.AlluxioMasterProcess}. The process will signal start, stop, promoting,
 * and demotion to the services registered to it when it sees fit.
 */
public interface MasterProcessMicroservice {
  /**
   * Starts the {@link MasterProcessMicroservice}. This method should make all the necessary service
   * initialization. It should also leave the service in a {@link alluxio.grpc.NodeState#STANDBY}
   * state, equivalent to the state found after the {@link #demote()} method.
   */
  public void start();

  /**
   * Promotes the {@link MasterProcessMicroservice} to {@link alluxio.grpc.NodeState#PRIMARY} state.
   */
  public void promote();

  /**
   * Demotes the {@link MasterProcessMicroservice} to {@link alluxio.grpc.NodeState#STANDBY} state.
   */
  public void demote();

  /**
   * Cleans up any open resources and stops the service gracefully.
   */
  public void stop();
}
