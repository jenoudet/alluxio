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
import site.ycsb.generator.SequentialGenerator;
import site.ycsb.generator.ZipfianGenerator;

import java.util.ArrayList;

/**
 * Governs the file structure parameters inside a JMH micro bench.
 */
@State(Scope.Benchmark)
public class BaseFileStructure {

  @Param({"0", "1", "10"})
  public int mDepth;

  @Param({"10", "100", "1000"})
  public int mFileCount;

  @Param({"SEQUENTIAL", "ZIPF"})
  public Distribution mDistribution;

  // each depth level needs its own file id generator
  public ArrayList<NumberGenerator> mFileGenerators = new ArrayList<>();
  public NumberGenerator mDepthGenerator;

  public enum Distribution { SEQUENTIAL, ZIPF }

  @Setup(Level.Trial)
  public void init() {
    switch (mDistribution) {
      case ZIPF:
        mDepthGenerator = new ZipfianGenerator(0, mDepth);
        for (int i = 0; i < mDepth + 1; i++) {
          mFileGenerators.add(new ZipfianGenerator(0, mFileCount - 1));
        }
        break;
      default:
        mDepthGenerator = new SequentialGenerator(0, mDepth);
        for (int i = 0; i < mDepth + 1; i++) {
          mFileGenerators.add(new SequentialGenerator(0, mFileCount - 1));
        }
    }
  }
}
