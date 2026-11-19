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

//! The RaBitQ code of one list, matching the JVM's writer bit for bit.
//!
//! A vector is centred on the list's mean, multiplied coordinate by coordinate by a random sign,
//! and transformed by the normalised Walsh-Hadamard transform. Each transformed coordinate is
//! then quantized to `1 << bits` levels and stored least-significant bit first; three `f32`
//! factors (length, code projection and centre dot) are stored beside the code.
//!
//! Every floating point step is the same operation in the same order as `RaBitQQuantizer` so that
//! a build reproduces the JVM's bytes: `f32` adds and multiplies in the transform and in the
//! centring, `f64` sums where the JVM sums in a `double`, and Java's `Math.round` rule
//! (`floor(x + 0.5)`) for the level of a coordinate.

use crate::java_random::JavaRandom;

/// The number of numbers kept per vector besides the code.
pub const FACTORS: usize = 3;

/// The smallest power of two at least as large as `value`.
pub fn next_power_of_two(value: usize) -> Result<usize, String> {
    if value == 0 {
        return Err("A dimension must be positive: 0".to_string());
    }

    let mut padded = 1usize;
    while padded < value {
        padded = padded
            .checked_mul(2)
            .ok_or_else(|| format!("The padded dimension overflows for {}", value))?;
    }

    Ok(padded)
}

/// The bytes one code takes: `ceil(padded * bits / 8)`.
pub fn code_bytes(padded: usize, bits: u8) -> usize {
    (padded * bits as usize).div_ceil(8)
}

/// The rotation signs, from the same generator the JVM seeds.
pub fn signs(seed: i64, length: usize) -> Vec<f32> {
    let mut random = JavaRandom::new(seed);
    let mut signs = Vec::with_capacity(length);
    for _ in 0..length {
        signs.push(if random.next_boolean() { 1.0 } else { -1.0 });
    }

    signs
}

/// The code of one list and the centre its vectors are measured from.
pub struct Quantizer {
    dimension: usize,
    padded: usize,
    bits: u8,
    center: Vec<f32>,
}

impl Quantizer {
    /// Trains on the vectors of one list, in the order the list holds them: the centre is their mean.
    pub fn train(vectors: &[&[f32]], dimension: usize, bits: u8) -> Result<Self, String> {
        if dimension == 0 {
            return Err("A quantizer dimension must be positive: 0".to_string());
        }

        let mut center = vec![0.0f32; dimension];
        for vector in vectors {
            if vector.len() != dimension {
                return Err(format!(
                    "A quantizer trained for {} dimensions was handed a vector of {}",
                    dimension,
                    vector.len()
                ));
            }

            for (slot, value) in center.iter_mut().zip(vector.iter()) {
                *slot += *value;
            }
        }

        if !vectors.is_empty() {
            let count = vectors.len() as f32;
            for value in center.iter_mut() {
                *value /= count;
            }
        }

        Ok(Self {
            dimension,
            padded: next_power_of_two(dimension)?,
            bits,
            center,
        })
    }

    pub fn center(&self) -> &[f32] {
        &self.center
    }

    pub fn padded(&self) -> usize {
        self.padded
    }

    pub fn bits(&self) -> u8 {
        self.bits
    }

    pub fn code_bytes(&self) -> usize {
        code_bytes(self.padded, self.bits)
    }

    /// Writes the code of one vector into `code`, and its three factors into `factors`.
    pub fn encode(
        &self,
        signs: &[f32],
        vector: &[f32],
        code: &mut [u8],
        factors: &mut [f32; FACTORS],
    ) -> Result<(), String> {
        if vector.len() != self.dimension {
            return Err(format!(
                "The quantizer was trained for {} dimensions but the vector has {}",
                self.dimension,
                vector.len()
            ));
        }

        if signs.len() != self.padded {
            return Err(format!(
                "The rotation has {} signs but the padded dimension is {}",
                signs.len(),
                self.padded
            ));
        }

        let code_bytes = self.code_bytes();
        if code.len() < code_bytes {
            return Err(format!(
                "A code of {} bytes does not fit the {} bytes handed to it",
                code_bytes,
                code.len()
            ));
        }
        code[..code_bytes].fill(0);

        let rotated = self.rotate(signs, vector);
        let norm = rotated
            .iter()
            .map(|value| (*value as f64) * (*value as f64))
            .sum::<f64>();
        let largest = rotated
            .iter()
            .map(|value| value.abs() as f64)
            .fold(0.0f64, f64::max);
        let levels = 1i32 << self.bits;
        let scale = if largest == 0.0 { 0.0 } else { 1.0 / largest };
        let root = norm.sqrt();
        let mut correction = 0.0f64;
        for (index, value) in rotated.iter().enumerate() {
            let relative = *value as f64 * scale;
            let level = java_round(
                ((relative + 1.0) * (levels - 1) as f64 / 2.0)
                    .min((levels - 1) as f64)
                    .max(0.0),
            ) as i32;
            self.set_level(code, index, level);
            correction += decode_level(level, levels) * (*value as f64 / root);
        }

        factors[0] = root as f32;
        factors[1] = if norm == 0.0 { 1.0 } else { correction as f32 };
        let mut dot_with_center = 0.0f64;
        for (value, center) in vector.iter().zip(self.center.iter()) {
            dot_with_center += ((*value - *center) as f64) * (*center as f64);
        }
        factors[2] = dot_with_center as f32;
        Ok(())
    }

    /// Centres the vector, flips its signs and transforms it.
    fn rotate(&self, signs: &[f32], vector: &[f32]) -> Vec<f32> {
        let mut data = vec![0.0f32; self.padded];
        for dimension in 0..self.dimension {
            data[dimension] = (vector[dimension] - self.center[dimension]) * signs[dimension];
        }

        hadamard(&mut data);
        data
    }

    /// Writes `level` at bits `index * bits` onwards, least-significant bit first.
    fn set_level(&self, code: &mut [u8], index: usize, level: i32) {
        let mask = (1i32 << self.bits) - 1;
        let local_bit = index * self.bits as usize;
        let shift = local_bit & 7;
        let first = local_bit >> 3;
        let mut word = code[first] as i32;
        let second = if shift + self.bits as usize > 8 {
            let second = (local_bit + self.bits as usize - 1) >> 3;
            word |= (code[second] as i32) << 8;
            second
        } else {
            usize::MAX
        };

        word &= !(mask << shift);
        word |= (level & mask) << shift;
        code[first] = word as u8;
        if second != usize::MAX {
            code[second] = (word >> 8) as u8;
        }
    }
}

/// `Math.round(double)`: the floor of the value plus one half.
fn java_round(value: f64) -> i64 {
    (value + 0.5).floor() as i64
}

/// The value a level stands for: `levels` of them spread evenly over the range.
fn decode_level(level: i32, levels: i32) -> f64 {
    if levels == 1 {
        0.0
    } else {
        -1.0 + 2.0 * level as f64 / (levels - 1) as f64
    }
}

/// The Walsh-Hadamard transform, normalised so that it is its own inverse.
fn hadamard(data: &mut [f32]) {
    let size = data.len();
    let mut length = 1;
    while length < size {
        let mut start = 0;
        while start < size {
            for index in start..start + length {
                let left = data[index];
                let right = data[index + length];
                data[index] = left + right;
                data[index + length] = left - right;
            }
            start += length << 1;
        }
        length <<= 1;
    }

    let scale = (1.0f64 / (size as f64).sqrt()) as f32;
    for value in data.iter_mut() {
        *value *= scale;
    }
}

#[cfg(test)]
mod tests {
    use super::{code_bytes, hadamard, next_power_of_two};

    #[test]
    fn pads_to_the_next_power_of_two() {
        assert_eq!(1, next_power_of_two(1).unwrap());
        assert_eq!(8, next_power_of_two(8).unwrap());
        assert_eq!(32, next_power_of_two(17).unwrap());
        assert_eq!(4096, next_power_of_two(2560).unwrap());
    }

    #[test]
    fn a_code_covers_every_coordinate() {
        assert_eq!(1, code_bytes(8, 1));
        assert_eq!(2, code_bytes(8, 2));
        assert_eq!(3, code_bytes(8, 3));
        assert_eq!(512, code_bytes(4096, 1));
        assert_eq!(1024, code_bytes(4096, 2));
    }

    #[test]
    fn the_transform_is_the_normalised_hadamard() {
        let mut data = vec![1.0f32, 2.0, 3.0, 4.0];
        hadamard(&mut data);
        // H4 / 2 applied to [1, 2, 3, 4]
        assert_eq!(vec![5.0f32, -1.0, -2.0, 0.0], data);
    }
}
