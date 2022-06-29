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

package alluxio;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import site.ycsb.generator.NumberGenerator;
import site.ycsb.generator.UniformLongGenerator;
import site.ycsb.generator.ZipfianGenerator;

/**
 * Governs the file structure parameters inside a JMH micro bench.
 */
@State(Scope.Benchmark)
public class BaseFileStructure {

  @Param({"0", "1", "10"})
  public int mDepth;

  @Param({"10", "100", "1000"})
  public int mFileCount;

  @Param({"UNIFORM", "ZIPF"})
  public Distribution mDistribution;

  public NumberGenerator mFileGenerator;
  public NumberGenerator mDepthGenerator;

  public enum Distribution { UNIFORM, ZIPF }

  @Setup(Level.Trial)
  public void init() {
    switch (mDistribution) {
      case ZIPF:
        mFileGenerator = new ZipfianGenerator(mFileCount);
        mDepthGenerator = new ZipfianGenerator(mDepth);
        break;
      default:
        mFileGenerator = new UniformLongGenerator(0, mFileCount - 1);
        mDepthGenerator = new UniformLongGenerator(0, mDepth);
    }
  }
}
