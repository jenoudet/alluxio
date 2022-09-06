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

package alluxio.master.microservices.metrics;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.microservices.MasterProcessMicroservice;

/**
 * Manages the enabling / disabling of metric sinks on an
 * {@link alluxio.master.AlluxioMasterProcess}.
 */
public abstract class MetricsSinkMicroservice implements MasterProcessMicroservice {

  /**
   * @return the microservice governing the behavior of the metrics sink for the alluxio master
   */
  public static MasterProcessMicroservice create() {
    if (Configuration.getBoolean(PropertyKey.STANDBY_MASTER_METRICS_SINK_ENABLED)) {
      return new AlwaysOnMetricsSinkMicroservice();
    }
    return new DefaultMetricsSinkMicroservice();
  }
}
