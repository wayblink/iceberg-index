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

//! Vectorised nearest-centroid passes that keep the scalar kernel's rounding.
//!
//! The obvious way to vectorise a distance is to split one row's sum over the lanes of the
//! dimension axis, but that changes the order the squares are added in, and the last bits of the
//! sum decide the list of a row whose centroids are near-tied. The payload then differs from the
//! one the scalar kernel writes, which is what the native kernel must not do.
//!
//! So the lanes are spent on the centroid axis instead: one dimension of one row is broadcast and
//! compared with several centroids at once, and each lane keeps its own accumulator that walks the
//! dimensions in order. Every lane therefore sums the same products in the same order as the
//! scalar path, and the square root of every lane is the scalar one. The centroid rows are
//! transposed to dimension-major so the vector load reads consecutive lanes.
//!
//! The multiply and the add are separate operations on purpose: a fused multiply-add rounds once
//! where the scalar path rounds twice.

/// How many centroids the host can walk at once in one `f64` vector.
pub fn lanes() -> usize {
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("avx512f") {
            return 8;
        }

        if std::arch::is_x86_feature_detected!("avx2") {
            return 4;
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("neon") {
            return 2;
        }
    }

    1
}

/// Lays the centroids out dimension-major, as `f64`: lane `list` of dimension `d` is at `d * L + list`.
pub(crate) fn transpose(centroids: &[Vec<f32>], list_count: usize, dimension: usize) -> Vec<f64> {
    let mut transposed = vec![0.0f64; list_count * dimension];
    for (list, centroid) in centroids.iter().enumerate() {
        for d in 0..dimension {
            transposed[d * list_count + list] = centroid[d] as f64;
        }
    }

    transposed
}

/// The distance of one row from every centroid, as the scalar kernel computes it.
pub(crate) fn distances(
    row: &[f32],
    transposed: &[f64],
    list_count: usize,
    lanes: usize,
    out: &mut [f64],
) {
    assert_eq!(out.len(), list_count, "one distance per centroid");
    let dimension = row.len();
    match lanes {
        #[cfg(target_arch = "x86_64")]
        8 => unsafe { distances_avx512(row, transposed, list_count, dimension, out) },
        #[cfg(target_arch = "x86_64")]
        4 => unsafe { distances_avx2(row, transposed, list_count, dimension, out) },
        #[cfg(target_arch = "aarch64")]
        2 => unsafe { distances_neon(row, transposed, list_count, dimension, out) },
        _ => distances_scalar(row, transposed, list_count, dimension, out),
    }
}

/// Puts every row under its nearest centroid.
pub fn nearest(rows: &[&[f32]], centroids: &[Vec<f32>], threads: usize) -> Vec<usize> {
    let mut assignment = vec![0usize; rows.len()];
    if rows.is_empty() || centroids.is_empty() {
        return assignment;
    }

    let list_count = centroids.len();
    let dimension = centroids[0].len();
    let transposed = transpose(centroids, list_count, dimension);
    let lanes = lanes();
    let threads = threads.max(1).min(rows.len().max(1));
    if threads == 1 {
        nearest_rows(rows, &transposed, list_count, lanes, &mut assignment);
        return assignment;
    }

    let chunk = rows.len().div_ceil(threads);
    std::thread::scope(|scope| {
        for (chunk_index, out) in assignment.chunks_mut(chunk).enumerate() {
            let start = chunk_index * chunk;
            let rows = &rows[start..start + out.len()];
            let transposed = &transposed;
            scope.spawn(move || nearest_rows(rows, transposed, list_count, lanes, out));
        }
    });

    assignment
}

fn nearest_rows(
    rows: &[&[f32]],
    transposed: &[f64],
    list_count: usize,
    lanes: usize,
    out: &mut [usize],
) {
    let dimension = transposed.len() / list_count;
    match lanes {
        #[cfg(target_arch = "x86_64")]
        8 => unsafe { nearest_blocks_avx512(rows, transposed, list_count, dimension, out) },
        #[cfg(target_arch = "x86_64")]
        4 => unsafe { nearest_blocks_avx2(rows, transposed, list_count, dimension, out) },
        #[cfg(target_arch = "aarch64")]
        2 => unsafe { nearest_blocks_neon(rows, transposed, list_count, dimension, out) },
        _ => nearest_rows_per_row(rows, transposed, list_count, lanes, out),
    }
}

/// The assignment of the rows a blocked kernel did not cover, one row at a time.
fn nearest_rows_per_row(
    rows: &[&[f32]],
    transposed: &[f64],
    list_count: usize,
    lanes: usize,
    out: &mut [usize],
) {
    let mut distances_of_row = vec![0.0f64; list_count];
    for (index, row) in rows.iter().enumerate() {
        distances(row, transposed, list_count, lanes, &mut distances_of_row);
        out[index] = nearest_of(&distances_of_row);
    }
}

/// The list of the strictly smallest distance, the earlier list on a tie.
fn nearest_of(distances_of_row: &[f64]) -> usize {
    let mut nearest = 0usize;
    let mut best = distances_of_row[0];
    for (list, distance) in distances_of_row.iter().enumerate().skip(1) {
        if *distance < best {
            best = *distance;
            nearest = list;
        }
    }

    nearest
}

/// The scalar sum for one list, reading the transposed layout: the same order as the row layout.
fn kmeans_distance_from(
    row: &[f32],
    transposed: &[f64],
    list_count: usize,
    list: usize,
    dimension: usize,
) -> f64 {
    let mut squared = 0.0f64;
    for d in 0..dimension {
        let difference = row[d] as f64 - transposed[d * list_count + list];
        squared += difference * difference;
    }

    squared.sqrt()
}

fn distances_scalar(
    row: &[f32],
    transposed: &[f64],
    list_count: usize,
    dimension: usize,
    out: &mut [f64],
) {
    for (list, slot) in out.iter_mut().take(list_count).enumerate() {
        *slot = kmeans_distance_from(row, transposed, list_count, list, dimension);
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn distances_avx2(
    row: &[f32],
    transposed: &[f64],
    list_count: usize,
    dimension: usize,
    out: &mut [f64],
) {
    use std::arch::x86_64::*;

    let groups = list_count / 4;
    let tail = groups * 4;
    for group in 0..groups {
        let base = group * 4;
        let mut accumulator = _mm256_setzero_pd();
        for d in 0..dimension {
            let value = _mm256_set1_pd(*row.get_unchecked(d) as f64);
            let centroid = _mm256_loadu_pd(transposed.as_ptr().add(d * list_count + base));
            let difference = _mm256_sub_pd(value, centroid);
            accumulator = _mm256_add_pd(accumulator, _mm256_mul_pd(difference, difference));
        }

        let mut lanes = [0.0f64; 4];
        _mm256_storeu_pd(lanes.as_mut_ptr(), accumulator);
        for (offset, value) in lanes.iter().enumerate() {
            *out.get_unchecked_mut(base + offset) = value.sqrt();
        }
    }

    for list in tail..list_count {
        *out.get_unchecked_mut(list) =
            kmeans_distance_from(row, transposed, list_count, list, dimension);
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn distances_avx512(
    row: &[f32],
    transposed: &[f64],
    list_count: usize,
    dimension: usize,
    out: &mut [f64],
) {
    use std::arch::x86_64::*;

    let groups = list_count / 8;
    let tail = groups * 8;
    for group in 0..groups {
        let base = group * 8;
        let mut accumulator = _mm512_setzero_pd();
        for d in 0..dimension {
            let value = _mm512_set1_pd(*row.get_unchecked(d) as f64);
            let centroid = _mm512_loadu_pd(transposed.as_ptr().add(d * list_count + base));
            let difference = _mm512_sub_pd(value, centroid);
            accumulator = _mm512_add_pd(accumulator, _mm512_mul_pd(difference, difference));
        }

        let mut lanes = [0.0f64; 8];
        _mm512_storeu_pd(lanes.as_mut_ptr(), accumulator);
        for (offset, value) in lanes.iter().enumerate() {
            *out.get_unchecked_mut(base + offset) = value.sqrt();
        }
    }

    for list in tail..list_count {
        *out.get_unchecked_mut(list) =
            kmeans_distance_from(row, transposed, list_count, list, dimension);
    }
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn distances_neon(
    row: &[f32],
    transposed: &[f64],
    list_count: usize,
    dimension: usize,
    out: &mut [f64],
) {
    use std::arch::aarch64::*;

    let groups = list_count / 2;
    let tail = groups * 2;
    for group in 0..groups {
        let base = group * 2;
        let mut accumulator = vdupq_n_f64(0.0);
        for d in 0..dimension {
            let value = vdupq_n_f64(*row.get_unchecked(d) as f64);
            let centroid = vld1q_f64(transposed.as_ptr().add(d * list_count + base));
            let difference = vsubq_f64(value, centroid);
            accumulator = vaddq_f64(accumulator, vmulq_f64(difference, difference));
        }

        let mut lanes = [0.0f64; 2];
        vst1q_f64(lanes.as_mut_ptr(), accumulator);
        *out.get_unchecked_mut(base) = lanes[0].sqrt();
        *out.get_unchecked_mut(base + 1) = lanes[1].sqrt();
    }

    for list in tail..list_count {
        *out.get_unchecked_mut(list) =
            kmeans_distance_from(row, transposed, list_count, list, dimension);
    }
}

/// The number of rows one blocked kernel walks against a centroid group at a time.
///
/// Eight rows keep the centroid group in registers for eight rows' worth of distance sums, which
/// is what stops the kernel from paying for a fresh stream of centroids per row, and eight
/// accumulators fit the register file of every target.
const ROWS_PER_BLOCK: usize = 8;

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn nearest_blocks_avx512(
    rows: &[&[f32]],
    transposed: &[f64],
    list_count: usize,
    dimension: usize,
    out: &mut [usize],
) {
    use std::arch::x86_64::*;

    let groups = list_count / 8;
    let tail = groups * 8;
    let blocks = rows.len() / ROWS_PER_BLOCK;
    for block in 0..blocks {
        let block_rows = &rows[block * ROWS_PER_BLOCK..(block + 1) * ROWS_PER_BLOCK];
        let block_out = &mut out[block * ROWS_PER_BLOCK..(block + 1) * ROWS_PER_BLOCK];
        let mut best = [f64::INFINITY; ROWS_PER_BLOCK];
        let mut nearest = [0usize; ROWS_PER_BLOCK];
        for group in 0..groups {
            let base = group * 8;
            let mut accumulators = [_mm512_setzero_pd(); ROWS_PER_BLOCK];
            for d in 0..dimension {
                let centroid = _mm512_loadu_pd(transposed.as_ptr().add(d * list_count + base));
                for (row, block_row) in block_rows.iter().enumerate() {
                    let value = _mm512_set1_pd(*block_row.get_unchecked(d) as f64);
                    let difference = _mm512_sub_pd(value, centroid);
                    accumulators[row] =
                        _mm512_add_pd(accumulators[row], _mm512_mul_pd(difference, difference));
                }
            }

            for (row, accumulator) in accumulators.iter().enumerate() {
                let mut lanes = [0.0f64; 8];
                _mm512_storeu_pd(lanes.as_mut_ptr(), *accumulator);
                for (offset, distance) in lanes.iter().enumerate() {
                    let distance = distance.sqrt();
                    if distance < best[row] {
                        best[row] = distance;
                        nearest[row] = base + offset;
                    }
                }
            }
        }

        for (row, block_row) in block_rows.iter().enumerate() {
            for list in tail..list_count {
                let distance =
                    kmeans_distance_from(block_row, transposed, list_count, list, dimension);
                if distance < best[row] {
                    best[row] = distance;
                    nearest[row] = list;
                }
            }

            block_out[row] = nearest[row];
        }
    }

    let done = blocks * ROWS_PER_BLOCK;
    nearest_rows_per_row(&rows[done..], transposed, list_count, 8, &mut out[done..]);
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn nearest_blocks_avx2(
    rows: &[&[f32]],
    transposed: &[f64],
    list_count: usize,
    dimension: usize,
    out: &mut [usize],
) {
    use std::arch::x86_64::*;

    let groups = list_count / 4;
    let tail = groups * 4;
    let blocks = rows.len() / ROWS_PER_BLOCK;
    for block in 0..blocks {
        let block_rows = &rows[block * ROWS_PER_BLOCK..(block + 1) * ROWS_PER_BLOCK];
        let block_out = &mut out[block * ROWS_PER_BLOCK..(block + 1) * ROWS_PER_BLOCK];
        let mut best = [f64::INFINITY; ROWS_PER_BLOCK];
        let mut nearest = [0usize; ROWS_PER_BLOCK];
        for group in 0..groups {
            let base = group * 4;
            let mut accumulators = [_mm256_setzero_pd(); ROWS_PER_BLOCK];
            for d in 0..dimension {
                let centroid = _mm256_loadu_pd(transposed.as_ptr().add(d * list_count + base));
                for (row, block_row) in block_rows.iter().enumerate() {
                    let value = _mm256_set1_pd(*block_row.get_unchecked(d) as f64);
                    let difference = _mm256_sub_pd(value, centroid);
                    accumulators[row] =
                        _mm256_add_pd(accumulators[row], _mm256_mul_pd(difference, difference));
                }
            }

            for (row, accumulator) in accumulators.iter().enumerate() {
                let mut lanes = [0.0f64; 4];
                _mm256_storeu_pd(lanes.as_mut_ptr(), *accumulator);
                for (offset, distance) in lanes.iter().enumerate() {
                    let distance = distance.sqrt();
                    if distance < best[row] {
                        best[row] = distance;
                        nearest[row] = base + offset;
                    }
                }
            }
        }

        for (row, block_row) in block_rows.iter().enumerate() {
            for list in tail..list_count {
                let distance =
                    kmeans_distance_from(block_row, transposed, list_count, list, dimension);
                if distance < best[row] {
                    best[row] = distance;
                    nearest[row] = list;
                }
            }

            block_out[row] = nearest[row];
        }
    }

    let done = blocks * ROWS_PER_BLOCK;
    nearest_rows_per_row(&rows[done..], transposed, list_count, 4, &mut out[done..]);
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn nearest_blocks_neon(
    rows: &[&[f32]],
    transposed: &[f64],
    list_count: usize,
    dimension: usize,
    out: &mut [usize],
) {
    use std::arch::aarch64::*;

    let groups = list_count / 2;
    let tail = groups * 2;
    let blocks = rows.len() / ROWS_PER_BLOCK;
    for block in 0..blocks {
        let block_rows = &rows[block * ROWS_PER_BLOCK..(block + 1) * ROWS_PER_BLOCK];
        let block_out = &mut out[block * ROWS_PER_BLOCK..(block + 1) * ROWS_PER_BLOCK];
        let mut best = [f64::INFINITY; ROWS_PER_BLOCK];
        let mut nearest = [0usize; ROWS_PER_BLOCK];
        for group in 0..groups {
            let base = group * 2;
            let mut accumulators = [vdupq_n_f64(0.0); ROWS_PER_BLOCK];
            for d in 0..dimension {
                let centroid = vld1q_f64(transposed.as_ptr().add(d * list_count + base));
                for (row, block_row) in block_rows.iter().enumerate() {
                    let value = vdupq_n_f64(*block_row.get_unchecked(d) as f64);
                    let difference = vsubq_f64(value, centroid);
                    accumulators[row] =
                        vaddq_f64(accumulators[row], vmulq_f64(difference, difference));
                }
            }

            for (row, accumulator) in accumulators.iter().enumerate() {
                let mut lanes = [0.0f64; 2];
                vst1q_f64(lanes.as_mut_ptr(), *accumulator);
                for (offset, distance) in lanes.iter().enumerate() {
                    let distance = distance.sqrt();
                    if distance < best[row] {
                        best[row] = distance;
                        nearest[row] = base + offset;
                    }
                }
            }
        }

        for (row, block_row) in block_rows.iter().enumerate() {
            for list in tail..list_count {
                let distance =
                    kmeans_distance_from(block_row, transposed, list_count, list, dimension);
                if distance < best[row] {
                    best[row] = distance;
                    nearest[row] = list;
                }
            }

            block_out[row] = nearest[row];
        }
    }

    let done = blocks * ROWS_PER_BLOCK;
    nearest_rows_per_row(&rows[done..], transposed, list_count, 2, &mut out[done..]);
}

#[cfg(test)]
mod tests {
    use super::{distances, lanes, nearest, transpose};
    use crate::kmeans;
    fn vectors(count: usize, dimension: usize) -> Vec<Vec<f32>> {
        let mut state = 0x2545F4914F6CDD1Du64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 11) as f32 / (1u64 << 53) as f32 - 0.5
        };

        (0..count)
            .map(|_| (0..dimension).map(|_| next() * 4.0).collect())
            .collect()
    }

    /// The scalar assignment, one row at a time, with the same tie break.
    fn reference_nearest(rows: &[Vec<f32>], centroids: &[Vec<f32>]) -> Vec<usize> {
        rows.iter()
            .map(|row| {
                let mut nearest = 0usize;
                let mut best = kmeans::distance(row, &centroids[0]);
                for (list, centroid) in centroids.iter().enumerate().skip(1) {
                    let distance = kmeans::distance(row, centroid);
                    if distance < best {
                        best = distance;
                        nearest = list;
                    }
                }

                nearest
            })
            .collect()
    }

    #[test]
    fn every_lane_width_sums_like_the_scalar_kernel() {
        let rows = vectors(7, 33);
        // nine centroids leave a tail for the four- and eight-lane kernels and for two lanes
        let centroids = vectors(9, 33);
        let transposed = transpose(&centroids, centroids.len(), 33);
        for row in &rows {
            for width in [1, 2, 4, 8] {
                let mut vectorised = vec![0.0f64; centroids.len()];
                distances(row, &transposed, centroids.len(), width, &mut vectorised);
                for (list, centroid) in centroids.iter().enumerate() {
                    let scalar = kmeans::distance(row, centroid);
                    assert_eq!(
                        scalar.to_bits(),
                        vectorised[list].to_bits(),
                        "row {:?}, list {}, width {}",
                        row,
                        list,
                        width
                    );
                }
            }
        }
    }

    #[test]
    fn the_assignment_is_the_scalar_one() {
        let mut rows = vectors(200, 64);
        // near-tied rows: a duplicate and a row equidistant from two centroids
        rows.push(rows[3].clone());
        rows.push(vec![0.0f32; 64]);
        let mut centroids = vectors(11, 64);
        centroids.push(centroids[2].clone());
        let expected = reference_nearest(&rows, &centroids);
        for threads in [1, 2, 4] {
            assert_eq!(
                expected,
                nearest(
                    &rows.iter().map(|row| row.as_slice()).collect::<Vec<_>>(),
                    &centroids,
                    threads
                ),
                "threads {}",
                threads
            );
        }
    }

    #[test]
    fn the_rows_before_between_and_after_full_blocks_match() {
        let centroids = vectors(13, 33);
        for row_count in [0usize, 1, 7, 8, 9, 16, 17] {
            let rows = vectors(row_count, 33);
            let expected = reference_nearest(&rows, &centroids);
            let row_slices: Vec<&[f32]> = rows.iter().map(|row| row.as_slice()).collect();
            assert_eq!(
                expected,
                nearest(&row_slices, &centroids, 2),
                "{} rows",
                row_count
            );
        }
    }

    #[test]
    fn the_host_reports_a_lane_width_it_can_use() {
        assert!([1, 2, 4, 8].contains(&lanes()), "lanes {}", lanes());
    }
}
