// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! `java.util.Random`, reproduced bit for bit.
//!
//! The quantizer's rotation signs come from a `Random` seeded with the index's seed, and the
//! payload has to match the JVM's writer byte for byte, so the same sequence has to be available
//! here. The generator is the specification's linear congruential one; only `next(bits)` is used.

const MULTIPLIER: u64 = 0x5DEECE66D;
const ADDEND: u64 = 0xB;
const MASK: u64 = (1 << 48) - 1;

/// The generator of `java.util.Random`.
pub struct JavaRandom {
    state: u64,
}

impl JavaRandom {
    /// The generator a `new java.util.Random(seed)` would hand out.
    pub fn new(seed: i64) -> Self {
        Self {
            state: (seed as u64 ^ MULTIPLIER) & MASK,
        }
    }

    /// `java.util.Random.next(int)`: the top `bits` of the next 48-bit state.
    fn next(&mut self, bits: u32) -> i32 {
        self.state = self.state.wrapping_mul(MULTIPLIER).wrapping_add(ADDEND) & MASK;
        (self.state >> (48 - bits)) as i32
    }

    /// `java.util.Random.nextBoolean()`.
    pub fn next_boolean(&mut self) -> bool {
        self.next(1) != 0
    }
}

#[cfg(test)]
mod tests {
    use super::JavaRandom;

    /// The expected bit strings come from running `new java.util.Random(seed).nextBoolean()` on a
    /// JDK 17 for the same seeds.
    #[test]
    fn reproduces_the_jvm_sequence() {
        let cases: [(i64, &str); 5] = [
            (0, "11011010110001111010000001001011"),
            (42, "10100101101000010111100011010010"),
            (
                0x9E37_79B9_7F4A_7C15u64 as i64,
                "10111110100000100110111110111011",
            ),
            (-1, "00011100101000011111101010000011"),
            (123_456_789, "11000010100000000001011001011010"),
        ];

        for (seed, expected) in cases {
            let mut random = JavaRandom::new(seed);
            let mut actual = String::new();
            for _ in 0..expected.len() {
                actual.push(if random.next_boolean() { '1' } else { '0' });
            }

            assert_eq!(expected, actual, "seed {}", seed);
        }
    }
}
