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
//!
//! Two implementations of the same arithmetic live here. [`Quantizer::encode`] is the fast one: it
//! reuses a per-thread scratch buffer instead of allocating per vector, looks the value of a level
//! up in a table instead of dividing per coordinate, writes a code a byte at a time instead of
//! through a read-modify-write for every coordinate, and walks the length sum, the largest
//! coordinate and the centre dot in one pass so that the three dependent `f64` sums overlap. It
//! keeps every sum in the order the JVM sums it, which is what makes its bytes equal.
//! [`Quantizer::encode_reference`] is the straightforward transcription of the JVM loop that the
//! fast path is tested against, and the path taken for a vector whose transformed coordinates are
//! not finite (where `NaN` propagation would depend on the width of the operations).

use std::cell::RefCell;

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

/// The buffers one thread reuses between vectors.
///
/// A build encodes every vector of a list with the same padded length, so one allocation per
/// thread serves the whole build; a `Vec` per vector would ask the allocator for 16 KiB and zero
/// it on every call.
#[derive(Default)]
struct Scratch {
    /// The centred, signed and transformed vector: `padded` `f32`.
    rotated: Vec<f32>,
    /// The level of each transformed coordinate, before it is packed into the code.
    levels: Vec<u8>,
}

impl Scratch {
    fn fit(&mut self, padded: usize) {
        if self.rotated.len() < padded {
            self.rotated.resize(padded, 0.0);
        }

        if self.levels.len() < padded {
            self.levels.resize(padded, 0);
        }
    }
}

thread_local! {
    /// One scratch per thread: [`Quantizer::encode`] is called from every build thread, and a
    /// buffer shared between them would need a lock on the hot path.
    static SCRATCH: RefCell<Scratch> = RefCell::new(Scratch::default());
}

/// The code of one list and the centre its vectors are measured from.
pub struct Quantizer {
    dimension: usize,
    padded: usize,
    bits: u8,
    center: Vec<f32>,
    /// `1 << bits`, the number of levels a coordinate is quantized to.
    levels: i32,
    /// The value a level stands for, indexed by level: [`decode_level`] of every level.
    decode: Vec<f64>,
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

        let levels = 1i32 << bits;
        let decode = (0..levels)
            .map(|level| decode_level(level, levels))
            .collect();
        Ok(Self {
            dimension,
            padded: next_power_of_two(dimension)?,
            bits,
            center,
            levels,
            decode,
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
        self.validate(signs, vector.len(), code.len())?;

        SCRATCH.with(|cell| {
            let mut scratch = cell.borrow_mut();
            scratch.fit(self.padded);
            self.encode_fast(signs, vector, code, factors, &mut scratch)
        })
    }

    fn validate(&self, signs: &[f32], vector_len: usize, code_len: usize) -> Result<(), String> {
        if vector_len != self.dimension {
            return Err(format!(
                "The quantizer was trained for {} dimensions but the vector has {}",
                self.dimension, vector_len
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
        if code_len < code_bytes {
            return Err(format!(
                "A code of {} bytes does not fit the {} bytes handed to it",
                code_bytes, code_len
            ));
        }

        Ok(())
    }

    /// The fast path: the same arithmetic as [`Quantizer::encode_reference`], in one pass per
    /// phase, with the sums kept in the order the JVM adds them.
    fn encode_fast(
        &self,
        signs: &[f32],
        vector: &[f32],
        code: &mut [u8],
        factors: &mut [f32; FACTORS],
        scratch: &mut Scratch,
    ) -> Result<(), String> {
        rotate_into(
            vector,
            &self.center,
            signs,
            &mut scratch.rotated[..self.padded],
        );
        hadamard(&mut scratch.rotated[..self.padded]);

        // The length sum, the largest coordinate and the centre dot are three independent `f64`
        // chains. Every one of them adds its terms in increasing coordinate order, which is what
        // the JVM does; running them side by side lets the processor overlap the three chains
        // instead of waiting for each in turn.
        let measured = measure(
            &scratch.rotated[..self.padded],
            self.dimension,
            vector,
            &self.center,
        );

        if !measured.finite {
            // `NaN` and infinities reach the code through rounding and saturating casts whose
            // result may depend on the width of the operation; the reference path is the one the
            // JVM's bytes are known to match, so hand the vector to it unchanged.
            return self.encode_reference(signs, vector, code, factors);
        }

        // the largest coordinate sets the grid; the scale is not stored because the projection of
        // the code absorbs it
        let max_level = (self.levels - 1) as f64;
        let scale = if measured.largest == 0.0 {
            0.0
        } else {
            1.0 / measured.largest
        };
        let root = measured.norm.sqrt();
        let correction = levels_into(
            &scratch.rotated[..self.padded],
            scale,
            max_level,
            root,
            &self.decode,
            &mut scratch.levels[..self.padded],
        );

        // the code occupies exactly `code_bytes` bytes and every one of them is written here, so
        // no clearing pass is needed
        pack_levels(
            &scratch.levels[..self.padded],
            self.bits,
            &mut code[..self.code_bytes()],
        );
        factors[0] = root as f32;
        factors[1] = if measured.norm == 0.0 {
            1.0
        } else {
            correction as f32
        };
        factors[2] = measured.dot as f32;
        Ok(())
    }

    /// The straightforward transcription of the JVM loop: the reference the fast path is tested
    /// against, and the path taken for a vector with a coordinate that is not finite.
    pub fn encode_reference(
        &self,
        signs: &[f32],
        vector: &[f32],
        code: &mut [u8],
        factors: &mut [f32; FACTORS],
    ) -> Result<(), String> {
        self.validate(signs, vector.len(), code.len())?;

        code[..self.code_bytes()].fill(0);
        let rotated = self.rotate(signs, vector);
        let norm = rotated
            .iter()
            .map(|value| (*value as f64) * (*value as f64))
            .sum::<f64>();
        // Java's `Math.max` propagates NaN where `f64::max` drops it, and that decides the grid a
        // coordinate is measured on: a NaN magnitude makes the scale NaN, which makes every level
        // zero, exactly as the JVM's loop does it
        let mut largest = 0.0f64;
        for value in &rotated {
            let magnitude = value.abs() as f64;
            largest = if largest.is_nan() || magnitude.is_nan() {
                f64::NAN
            } else {
                largest.max(magnitude)
            };
        }
        let levels = self.levels;
        let scale = if largest == 0.0 { 0.0 } else { 1.0 / largest };
        let root = norm.sqrt();
        let mut correction = 0.0f64;
        for (index, value) in rotated.iter().enumerate() {
            let relative = *value as f64 * scale;
            // Java's `Math.min`/`Math.max` propagate NaN and `Math.round(NaN)` is 0; `f64::min`
            // and `f64::max` would keep the other operand and put the coordinate on the last
            // level, which is a different byte for a vector whose transform overflowed
            let level = if relative.is_nan() {
                0
            } else {
                java_round(
                    ((relative + 1.0) * (levels - 1) as f64 / 2.0)
                        .min((levels - 1) as f64)
                        .max(0.0),
                ) as i32
            };
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

/// The three `f64` sums of one encode, measured in one pass.
struct Measured {
    norm: f64,
    largest: f64,
    dot: f64,
    finite: bool,
}

/// Centres the vector, flips its signs and writes the result; the padding is left zero.
fn rotate_into(vector: &[f32], center: &[f32], signs: &[f32], rotated: &mut [f32]) {
    #[cfg(target_arch = "x86_64")]
    {
        if x86::available() {
            unsafe { x86::rotate(vector, center, signs, rotated) };
            return;
        }
    }

    let dimension = vector.len();
    for index in 0..dimension {
        rotated[index] = (vector[index] - center[index]) * signs[index];
    }
    rotated[dimension..].fill(0.0);
}

/// The length, the largest magnitude and the centre dot of a transformed vector, plus whether
/// every coordinate was finite.
fn measure(rotated: &[f32], dimension: usize, vector: &[f32], center: &[f32]) -> Measured {
    #[cfg(target_arch = "x86_64")]
    {
        if x86::available() {
            return unsafe { x86::measure(rotated, dimension, vector, center) };
        }
    }

    let mut norm = 0.0f64;
    let mut largest = 0.0f64;
    let mut dot = 0.0f64;
    let mut finite = true;
    for index in 0..dimension {
        let value = rotated[index];
        norm += (value as f64) * (value as f64);
        largest = largest.max(value.abs() as f64);
        finite &= value.is_finite();
        let center = center[index];
        dot += ((vector[index] - center) as f64) * (center as f64);
    }

    for value in &rotated[dimension..] {
        norm += (*value as f64) * (*value as f64);
        largest = largest.max(value.abs() as f64);
        finite &= value.is_finite();
    }

    Measured {
        norm,
        largest,
        dot,
        finite,
    }
}

/// The level of every transformed coordinate, and the projection of the code onto the direction
/// it stands for.
fn levels_into(
    rotated: &[f32],
    scale: f64,
    max_level: f64,
    root: f64,
    decode: &[f64],
    levels: &mut [u8],
) -> f64 {
    #[cfg(target_arch = "x86_64")]
    {
        if x86::available() {
            return unsafe { x86::levels(rotated, scale, max_level, root, decode, levels) };
        }
    }

    let mut correction = 0.0f64;
    for (index, value) in rotated.iter().enumerate() {
        let relative = *value as f64 * scale;
        let level = java_round(((relative + 1.0) * max_level * 0.5).min(max_level).max(0.0));
        levels[index] = level as u8;
        correction += decode[level as usize] * (*value as f64 / root);
    }
    correction
}

/// The x86-64 kernels: sixteen `f32` lanes for the transform and the centring, eight `f64` lanes
/// for the sums. When the host has no AVX-512 the scalar code in the callers runs instead.
#[cfg(target_arch = "x86_64")]
mod x86 {
    use std::arch::x86_64::*;

    use super::Measured;

    /// Whether this host runs the kernels below. The level narrowing needs AVX-512VL, which every
    /// server part that has AVX-512 also has; Knights Landing does not and takes the scalar path.
    pub(super) fn available() -> bool {
        std::arch::is_x86_feature_detected!("avx512f")
            && std::arch::is_x86_feature_detected!("avx512vl")
    }

    #[target_feature(enable = "avx512f")]
    pub(super) unsafe fn rotate(
        vector: &[f32],
        center: &[f32],
        signs: &[f32],
        rotated: &mut [f32],
    ) {
        let dimension = vector.len();
        let mut index = 0;
        while index + 16 <= dimension {
            let values = _mm512_loadu_ps(vector.as_ptr().add(index));
            let centers = _mm512_loadu_ps(center.as_ptr().add(index));
            let rotation = _mm512_loadu_ps(signs.as_ptr().add(index));
            let centred = _mm512_mul_ps(_mm512_sub_ps(values, centers), rotation);
            _mm512_storeu_ps(rotated.as_mut_ptr().add(index), centred);
            index += 16;
        }

        while index < dimension {
            rotated[index] = (vector[index] - center[index]) * signs[index];
            index += 1;
        }

        rotated[dimension..].fill(0.0);
    }

    /// The lane permutation that swaps the two halves of every butterfly of a stage, and the
    /// permutation that gathers the sums and the differences back into coordinate order. For the
    /// four stages shorter than a vector, the partner of a lane is another lane of the same
    /// vector; `vpermps` brings it over and `vpermt2ps` puts the result back, so sixteen
    /// coordinates are transformed per instruction pair instead of two.
    const SWAP_1: [i32; 16] = [1, 0, 3, 2, 5, 4, 7, 6, 9, 8, 11, 10, 13, 12, 15, 14];
    const SWAP_2: [i32; 16] = [2, 3, 0, 1, 6, 7, 4, 5, 10, 11, 8, 9, 14, 15, 12, 13];
    const SWAP_4: [i32; 16] = [4, 5, 6, 7, 0, 1, 2, 3, 12, 13, 14, 15, 8, 9, 10, 11];
    const SWAP_8: [i32; 16] = [8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7];
    const COMBINE_1: [i32; 16] = [0, 16, 2, 18, 4, 20, 6, 22, 8, 24, 10, 26, 12, 28, 14, 30];
    const COMBINE_2: [i32; 16] = [0, 1, 16, 17, 4, 5, 20, 21, 8, 9, 24, 25, 12, 13, 28, 29];
    const COMBINE_4: [i32; 16] = [0, 1, 2, 3, 16, 17, 18, 19, 8, 9, 10, 11, 24, 25, 26, 27];
    const COMBINE_8: [i32; 16] = [0, 1, 2, 3, 4, 5, 6, 7, 16, 17, 18, 19, 20, 21, 22, 23];

    #[inline]
    #[target_feature(enable = "avx512f")]
    unsafe fn short_stage(pointer: *mut f32, size: usize, swap: &[i32; 16], combine: &[i32; 16]) {
        let exchange = _mm512_castps_si512(_mm512_loadu_ps(swap.as_ptr() as *const f32));
        let gather = _mm512_castps_si512(_mm512_loadu_ps(combine.as_ptr() as *const f32));
        for offset in (0..size).step_by(16) {
            let values = _mm512_loadu_ps(pointer.add(offset));
            let partner = _mm512_permutexvar_ps(exchange, values);
            let sums = _mm512_add_ps(values, partner);
            let differences = _mm512_sub_ps(values, partner);
            let result = _mm512_permutex2var_ps(sums, gather, differences);
            _mm512_storeu_ps(pointer.add(offset), result);
        }
    }

    #[target_feature(enable = "avx512f")]
    pub(super) unsafe fn hadamard(data: &mut [f32]) {
        let size = data.len();
        let pointer = data.as_mut_ptr();

        // Every block of these stages lies inside one aligned chunk of sixteen coordinates, so a
        // chunk is transformed on its own.
        short_stage(pointer, size, &SWAP_1, &COMBINE_1);
        short_stage(pointer, size, &SWAP_2, &COMBINE_2);
        short_stage(pointer, size, &SWAP_4, &COMBINE_4);
        short_stage(pointer, size, &SWAP_8, &COMBINE_8);

        // From here a butterfly pair is a vector apart: two loads, an add and a subtract.
        let mut length = 16;
        while length < size {
            let mut start = 0;
            while start < size {
                let mut index = start;
                while index + 16 <= start + length {
                    let left = _mm512_loadu_ps(pointer.add(index));
                    let right = _mm512_loadu_ps(pointer.add(index + length));
                    _mm512_storeu_ps(pointer.add(index), _mm512_add_ps(left, right));
                    _mm512_storeu_ps(pointer.add(index + length), _mm512_sub_ps(left, right));
                    index += 16;
                }
                start += length << 1;
            }
            length <<= 1;
        }

        let scale = _mm512_set1_ps((1.0f64 / (size as f64).sqrt()) as f32);
        for offset in (0..size).step_by(16) {
            let values = _mm512_loadu_ps(pointer.add(offset));
            _mm512_storeu_ps(pointer.add(offset), _mm512_mul_ps(values, scale));
        }
    }

    #[target_feature(enable = "avx512f")]
    pub(super) unsafe fn measure(
        rotated: &[f32],
        dimension: usize,
        vector: &[f32],
        center: &[f32],
    ) -> Measured {
        let infinity = _mm512_set1_pd(f64::INFINITY);
        let sign = _mm512_set1_pd(f64::from_bits(0x8000_0000_0000_0000));
        let mut norm = 0.0f64;
        let mut dot = 0.0f64;
        let mut largest = _mm512_setzero_pd();
        let mut finite = 0xFFu8;
        let mut squares = [0.0f64; 8];
        let mut products = [0.0f64; 8];

        // the sums are added one lane after the other, in coordinate order, exactly as the JVM
        // adds them; the products themselves and the magnitude compare are lane-wise, so they
        // can be done eight at a time without changing a bit
        let mut index = 0;
        while index + 8 <= dimension {
            let values = _mm512_cvtps_pd(_mm256_loadu_ps(rotated.as_ptr().add(index)));
            let squared = _mm512_mul_pd(values, values);
            _mm512_storeu_pd(squares.as_mut_ptr(), squared);
            norm += squares[0];
            norm += squares[1];
            norm += squares[2];
            norm += squares[3];
            norm += squares[4];
            norm += squares[5];
            norm += squares[6];
            norm += squares[7];

            let magnitudes = _mm512_andnot_pd(sign, values);
            largest = _mm512_max_pd(largest, magnitudes);
            finite &= _mm512_cmp_pd_mask(magnitudes, infinity, _CMP_LT_OQ);

            let differences = _mm256_sub_ps(
                _mm256_loadu_ps(vector.as_ptr().add(index)),
                _mm256_loadu_ps(center.as_ptr().add(index)),
            );
            let product = _mm512_mul_pd(
                _mm512_cvtps_pd(differences),
                _mm512_cvtps_pd(_mm256_loadu_ps(center.as_ptr().add(index))),
            );
            _mm512_storeu_pd(products.as_mut_ptr(), product);
            dot += products[0];
            dot += products[1];
            dot += products[2];
            dot += products[3];
            dot += products[4];
            dot += products[5];
            dot += products[6];
            dot += products[7];
            index += 8;
        }

        // the coordinates past the vector width but still inside the dimension also carry the
        // centre dot
        while index < dimension {
            let value = rotated[index];
            norm += (value as f64) * (value as f64);
            largest = _mm512_max_pd(largest, _mm512_set1_pd(value.abs() as f64));
            if !value.is_finite() {
                finite = 0;
            }
            let center = center[index];
            dot += ((vector[index] - center) as f64) * (center as f64);
            index += 1;
        }

        // the padding is past the dimension, so it is summed but not dotted
        while index + 8 <= rotated.len() {
            let values = _mm512_cvtps_pd(_mm256_loadu_ps(rotated.as_ptr().add(index)));
            let squared = _mm512_mul_pd(values, values);
            _mm512_storeu_pd(squares.as_mut_ptr(), squared);
            norm += squares[0];
            norm += squares[1];
            norm += squares[2];
            norm += squares[3];
            norm += squares[4];
            norm += squares[5];
            norm += squares[6];
            norm += squares[7];

            let magnitudes = _mm512_andnot_pd(sign, values);
            largest = _mm512_max_pd(largest, magnitudes);
            finite &= _mm512_cmp_pd_mask(magnitudes, infinity, _CMP_LT_OQ);
            index += 8;
        }

        while index < rotated.len() {
            let value = rotated[index];
            norm += (value as f64) * (value as f64);
            largest = _mm512_max_pd(largest, _mm512_set1_pd(value.abs() as f64));
            if !value.is_finite() {
                finite = 0;
            }
            index += 1;
        }

        // the largest lane is the largest coordinate; the order the scalar fold visits them in
        // cannot change a maximum, and a coordinate that is not finite falls back above
        let mut lanes = [0.0f64; 8];
        _mm512_storeu_pd(lanes.as_mut_ptr(), largest);
        let largest = lanes.iter().fold(0.0f64, |left, right| left.max(*right));

        Measured {
            norm,
            dot,
            largest,
            finite: finite == 0xFF,
        }
    }

    #[target_feature(enable = "avx512f,avx512vl")]
    pub(super) unsafe fn levels(
        rotated: &[f32],
        scale: f64,
        max_level: f64,
        root: f64,
        decode: &[f64],
        levels: &mut [u8],
    ) -> f64 {
        let scale_vector = _mm512_set1_pd(scale);
        let one = _mm512_set1_pd(1.0);
        let half = _mm512_set1_pd(0.5);
        let maximum = _mm512_set1_pd(max_level);
        let zero = _mm512_setzero_pd();
        let divisor = _mm512_set1_pd(root);
        let mut correction = 0.0f64;
        let mut terms = [0.0f64; 8];

        let mut index = 0;
        while index + 8 <= rotated.len() {
            let values = _mm512_cvtps_pd(_mm256_loadu_ps(rotated.as_ptr().add(index)));
            let relative = _mm512_mul_pd(values, scale_vector);
            let position =
                _mm512_mul_pd(_mm512_mul_pd(_mm512_add_pd(relative, one), maximum), half);
            let clamped = _mm512_max_pd(_mm512_min_pd(position, maximum), zero);
            // the clamp leaves the position non-negative, where truncating a value plus one half
            // is the floor of it plus one half: Java's rounding rule without a second rounding
            // instruction
            let level = _mm512_cvttpd_epi32(_mm512_add_pd(clamped, half));
            let bytes = _mm256_cvtepi32_epi8(level);
            _mm_storel_epi64(levels.as_mut_ptr().add(index) as *mut __m128i, bytes);

            let decoded = _mm512_i32gather_pd(level, decode.as_ptr(), 8);
            let quotient = _mm512_div_pd(values, divisor);
            let term = _mm512_mul_pd(decoded, quotient);
            _mm512_storeu_pd(terms.as_mut_ptr(), term);
            correction += terms[0];
            correction += terms[1];
            correction += terms[2];
            correction += terms[3];
            correction += terms[4];
            correction += terms[5];
            correction += terms[6];
            correction += terms[7];
            index += 8;
        }

        // a padded length below eight (a dimension of at most four) has no full vector
        while index < rotated.len() {
            let value = rotated[index] as f64;
            let relative = value * scale;
            let position = ((relative + 1.0) * max_level * 0.5).min(max_level).max(0.0);
            let level = (position + 0.5).floor() as i64;
            levels[index] = level as u8;
            correction += decode[level as usize] * (value / root);
            index += 1;
        }

        correction
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

/// Writes the levels of one code, least-significant bit first: coordinate `i` occupies bits
/// `i * bits .. i * bits + bits` of the code, which is the order [`Quantizer::set_level`] writes
/// them in.
fn pack_levels(levels: &[u8], bits: u8, code: &mut [u8]) {
    let bits = bits as usize;
    let total_bits = levels.len() * bits;
    debug_assert_eq!(total_bits.div_ceil(8), code.len());

    // Bits to the left of a byte boundary would need a second multiply, so only the widths that
    // divide a byte take the wide path; 3, 5, 6 and 7 fall through to the accumulator below.
    let (whole, tail) = if bits == 8 {
        code[..levels.len()].copy_from_slice(levels);
        return;
    } else if matches!(bits, 1 | 2 | 4) {
        let group = 8 / bits;
        let whole = levels.len() - levels.len() % group;
        (whole, &levels[whole..])
    } else {
        (0, levels)
    };

    let mut position = 0;
    match bits {
        1 => {
            // One multiply gathers the eight bits of a byte from eight level bytes: the level of
            // coordinate `i` lands in bit `i` of the top byte of the product, and no two levels
            // can carry into each other because the packed byte is at most 255.
            const MULTIPLIER: u64 = 0x0102_0408_1020_4080;
            for chunk in levels[..whole].chunks_exact(8) {
                let word = u64::from_le_bytes(chunk.try_into().expect("eight levels"));
                code[position] = (word.wrapping_mul(MULTIPLIER) >> 56) as u8;
                position += 1;
            }
        }
        2 => {
            const MULTIPLIER: u64 = 0x0104_1040_0000_0000;
            for chunk in levels[..whole].chunks_exact(4) {
                let word = u32::from_le_bytes(chunk.try_into().expect("four levels")) as u64;
                code[position] = (word.wrapping_mul(MULTIPLIER) >> 56) as u8;
                position += 1;
            }
        }
        4 => {
            const MULTIPLIER: u64 = 0x0110_0000_0000_0000;
            for chunk in levels[..whole].chunks_exact(2) {
                let word = u16::from_le_bytes(chunk.try_into().expect("two levels")) as u64;
                code[position] = (word.wrapping_mul(MULTIPLIER) >> 56) as u8;
                position += 1;
            }
        }
        _ => {}
    }

    let mut accumulator = 0u64;
    let mut held = 0usize;
    for &level in tail {
        accumulator |= (level as u64) << held;
        held += bits;
        while held >= 8 {
            code[position] = accumulator as u8;
            position += 1;
            accumulator >>= 8;
            held -= 8;
        }
    }

    if held > 0 {
        code[position] = accumulator as u8;
    }
}

/// The Walsh-Hadamard transform, normalised so that it is its own inverse.
fn hadamard(data: &mut [f32]) {
    // The padded length is a power of two the caller knows; spelling the common ones out lets the
    // compiler see the trip count of every butterfly and vectorise it. Other lengths (a dimension
    // of 5 pads to 8, for instance) keep the runtime loop.
    #[cfg(target_arch = "x86_64")]
    {
        if data.len() >= 16 && x86::available() {
            unsafe { x86::hadamard(data) };
            return;
        }
    }

    match data.len() {
        1024 => hadamard_sized::<1024>(data.try_into().expect("padded length")),
        2048 => hadamard_sized::<2048>(data.try_into().expect("padded length")),
        4096 => hadamard_sized::<4096>(data.try_into().expect("padded length")),
        8192 => hadamard_sized::<8192>(data.try_into().expect("padded length")),
        16384 => hadamard_sized::<16384>(data.try_into().expect("padded length")),
        _ => hadamard_runtime(data),
    }
}

/// The same butterflies with every length and bound a constant, so the compiler can vectorise.
fn hadamard_sized<const SIZE: usize>(data: &mut [f32; SIZE]) {
    let mut length = 1;
    while length < SIZE {
        let mut start = 0;
        while start < SIZE {
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

    let scale = (1.0f64 / (SIZE as f64).sqrt()) as f32;
    for value in data.iter_mut() {
        *value *= scale;
    }
}

fn hadamard_runtime(data: &mut [f32]) {
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
    use super::{
        code_bytes, decode_level, hadamard, next_power_of_two, pack_levels, Quantizer, FACTORS,
    };

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

    /// A deterministic generator, so a failing case can be reproduced from its seed.
    struct Rng(u64);

    impl Rng {
        fn next_u32(&mut self) -> u32 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 as u32
        }

        fn next_f32(&mut self) -> f32 {
            let unit = (self.next_u32() >> 8) as f32 / (1u32 << 24) as f32;
            unit * 2.0 - 1.0
        }
    }

    fn vectors(seed: u64, count: usize, dimension: usize, scale: f32) -> Vec<Vec<f32>> {
        let mut random = Rng(seed);
        (0..count)
            .map(|_| (0..dimension).map(|_| random.next_f32() * scale).collect())
            .collect()
    }

    fn same_bytes(dimension: usize, bits: u8, vectors: &[Vec<f32>], signs: &[f32]) {
        let borrowed: Vec<&[f32]> = vectors.iter().map(|vector| vector.as_slice()).collect();
        let quantizer = Quantizer::train(&borrowed, dimension, bits).unwrap();
        let mut code = vec![0u8; quantizer.code_bytes()];
        let mut factors = [0.0f32; FACTORS];
        let mut reference = vec![0u8; quantizer.code_bytes()];
        let mut reference_factors = [0.0f32; FACTORS];
        for vector in vectors {
            quantizer
                .encode(signs, vector, &mut code, &mut factors)
                .unwrap();
            quantizer
                .encode_reference(signs, vector, &mut reference, &mut reference_factors)
                .unwrap();
            assert_eq!(
                reference, code,
                "code differs for {} dimensions at {} bits",
                dimension, bits
            );
            for (index, (fast, reference)) in factors.iter().zip(reference_factors).enumerate() {
                assert_eq!(
                    fast.to_bits(),
                    reference.to_bits(),
                    "factor {} differs for {} dimensions at {} bits: {} against {}",
                    index,
                    dimension,
                    bits,
                    fast,
                    reference
                );
            }
        }
    }

    #[test]
    fn the_fast_path_writes_the_reference_bytes() {
        let dimensions = [
            1, 2, 3, 4, 5, 6, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 100, 127, 128, 255, 256,
            257, 511, 1000, 2560, 4097, 10000,
        ];
        for dimension in dimensions {
            for bits in 1..=8u8 {
                let padded = next_power_of_two(dimension).unwrap();
                let signs = super::signs(0x9E3779B97F4A7C15u64 as i64, padded);
                let vectors = vectors(
                    dimension as u64 * 31 + bits as u64,
                    4,
                    dimension,
                    1.0 + bits as f32,
                );
                same_bytes(dimension, bits, &vectors, &signs);
            }
        }
    }

    #[test]
    fn the_fast_path_matches_for_degenerate_vectors() {
        for dimension in [1, 2, 5, 8, 17, 64, 300] {
            for bits in [1, 2, 3, 4, 5, 8] {
                let padded = next_power_of_two(dimension).unwrap();
                let signs = super::signs(7, padded);
                let zero = vec![vec![0.0f32; dimension]];
                same_bytes(dimension, bits, &zero, &signs);

                let mut ones = vec![1.0f32; dimension];
                ones[dimension / 2] = -0.0;
                same_bytes(dimension, bits, &[ones.clone()], &signs);

                let tiny = vec![vec![f32::MIN_POSITIVE / 3.0; dimension]];
                same_bytes(dimension, bits, &tiny, &signs);

                let constant = vec![vec![std::f32::consts::PI; dimension]];
                same_bytes(dimension, bits, &constant, &signs);

                // the transform overflows `f32` here, so the fast path hands the vector to the
                // reference; the two must still agree, and the reference is the JVM's loop
                let overflow = vec![
                    vec![0.0f32; dimension],
                    (0..dimension)
                        .map(|index| if index % 2 == 0 { 3.0e38 } else { -3.0e38 })
                        .collect::<Vec<f32>>(),
                ];
                same_bytes(dimension, bits, &overflow, &signs);

                // a coordinate that is not finite cannot be rounded; the reference path decides
                let not_a_number = vec![
                    vec![0.0f32; dimension],
                    (0..dimension)
                        .map(|index| if index % 3 == 0 { f32::NAN } else { 1.0 })
                        .collect::<Vec<f32>>(),
                ];
                same_bytes(dimension, bits, &not_a_number, &signs);
            }
        }
    }

    #[test]
    fn an_overflowed_coordinate_takes_the_jvm_level() {
        // two dimensions pad to two; seed 0 gives both signs +1 and one bit gives two levels. The
        // transform of [3e38, 3e38] overflows to [inf, 0], so the grid is [NaN, 0.5] and Java's
        // Math.round(NaN) is 0: the code is level 1, level 0 -> 0b10. A min/max that keeps the
        // other operand on NaN would put the first coordinate on the last level -> 0b11.
        let signs = super::signs(0, 2);
        let quantizer = Quantizer::train(&[&[0.0f32, 0.0][..]], 2, 1).unwrap();
        let mut code = vec![0u8; quantizer.code_bytes()];
        let mut factors = [0.0f32; FACTORS];
        let mut reference = vec![0u8; quantizer.code_bytes()];
        let mut reference_factors = [0.0f32; FACTORS];
        for vector in [[3.0e38f32, 3.0e38f32], [-3.0e38f32, -3.0e38f32]] {
            quantizer
                .encode(&signs, &vector, &mut code, &mut factors)
                .unwrap();
            quantizer
                .encode_reference(&signs, &vector, &mut reference, &mut reference_factors)
                .unwrap();
            assert_eq!(vec![2u8], code, "the JVM code of {:?}", vector);
            assert_eq!(
                reference, code,
                "the fast path hands a non-finite transform to the reference"
            );
        }
    }

    #[test]
    fn a_vector_of_ones_has_no_length() {
        let vector = vec![1.0f32; 16];
        let signs = super::signs(3, 16);
        let quantizer = Quantizer::train(&[vector.as_slice()], 16, 4).unwrap();
        let mut code = vec![0u8; quantizer.code_bytes()];
        let mut factors = [0.0f32; FACTORS];
        quantizer
            .encode(&signs, &vector, &mut code, &mut factors)
            .unwrap();
        // the centred vector is all zero, so the length is zero and the projection is one
        assert_eq!(0.0f32, factors[0]);
        assert_eq!(1.0f32, factors[1]);
    }

    #[test]
    fn a_packed_code_is_the_levels_least_significant_bit_first() {
        for bits in 1..=8u8 {
            for count in [1usize, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 32, 33] {
                let levels = (0..count)
                    .map(|index| ((index * 37 + 11) as u32 % (1u32 << bits)) as u8)
                    .collect::<Vec<u8>>();
                let mut code = vec![0u8; code_bytes(count, bits)];
                pack_levels(&levels, bits, &mut code);

                // an independent packer: append the level's bits one at a time
                let mut expected = vec![0u8; code_bytes(count, bits)];
                for (index, level) in levels.iter().enumerate() {
                    for bit in 0..bits as usize {
                        if level & (1 << bit) != 0 {
                            let position = index * bits as usize + bit;
                            expected[position >> 3] |= 1 << (position & 7);
                        }
                    }
                }

                assert_eq!(
                    expected, code,
                    "packing differs for {} levels at {} bits",
                    count, bits
                );
            }
        }
    }

    #[test]
    fn every_level_decodes_to_the_reference_value() {
        for bits in 1..=8u8 {
            let levels = 1i32 << bits;
            let quantizer = Quantizer::train(&[&[0.0f32; 4][..]], 4, bits).unwrap();
            assert_eq!(levels as usize, quantizer.decode.len());
            for level in 0..levels {
                assert_eq!(
                    decode_level(level, levels).to_bits(),
                    quantizer.decode[level as usize].to_bits()
                );
            }
        }
    }
}
