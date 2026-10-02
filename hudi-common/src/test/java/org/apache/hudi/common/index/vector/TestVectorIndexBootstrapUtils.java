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

package org.apache.hudi.common.index.vector;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestVectorIndexBootstrapUtils {

  @Test
  void splitsInterleavedBitsIntoPostingPlanes() {
    // Two lower bits per dimension: 01, 10, 11, 00 (least-significant bit first).
    byte[] interleaved = {(byte) 0x39};

    byte[] planes = VectorIndexBootstrapUtils.splitExPlanes(interleaved, 2, 4, 8);

    // Plane 0 holds the most-significant lower bit (dims 1 and 2), plane 1 the least (dims 0 and 2).
    byte[] expected = new byte[16];
    expected[0] = 0x06;
    expected[8] = 0x05;
    assertArrayEquals(expected, planes);
  }

  @Test
  void planesCarryScorerWeights() {
    // Scorers weight plane p by 2^(planeCount - 1 - p); the weighted planes must rebuild each level.
    int planeCount = 3;
    int dimension = 8;
    int rowBytes = 8;
    int[] levels = {0, 1, 2, 3, 4, 5, 6, 7};
    byte[] interleaved = new byte[(dimension * planeCount + 7) / 8];
    for (int dimensionIndex = 0; dimensionIndex < dimension; dimensionIndex++) {
      for (int bit = 0; bit < planeCount; bit++) {
        if ((levels[dimensionIndex] & (1 << bit)) != 0) {
          int sourceBit = dimensionIndex * planeCount + bit;
          interleaved[sourceBit >>> 3] |= (byte) (1 << (sourceBit & 7));
        }
      }
    }

    byte[] planes = VectorIndexBootstrapUtils.splitExPlanes(interleaved, planeCount, dimension, rowBytes);

    for (int dimensionIndex = 0; dimensionIndex < dimension; dimensionIndex++) {
      int level = 0;
      for (int plane = 0; plane < planeCount; plane++) {
        int bitIndex = plane * rowBytes * Byte.SIZE + dimensionIndex;
        if ((planes[bitIndex >>> 3] & (1 << (bitIndex & 7))) != 0) {
          level += 1 << (planeCount - 1 - plane);
        }
      }
      assertEquals(levels[dimensionIndex], level, "dimension " + dimensionIndex);
    }
  }

  @Test
  void returnsNoPlanesForBinaryEncoding() {
    assertArrayEquals(new byte[0],
        VectorIndexBootstrapUtils.splitExPlanes(null, 0, 4, 8));
  }

  @Test
  void rejectsPackedCodeWithWrongSize() {
    assertThrows(IllegalArgumentException.class,
        () -> VectorIndexBootstrapUtils.splitExPlanes(new byte[2], 2, 4, 8));
  }
}
