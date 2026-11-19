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
//! compared with eight centroids at once, and each lane keeps its own accumulator that walks the
//! dimensions in order. Every lane therefore sums the same products in the same order as the
//! scalar path, and the square root of every lane is the scalar one. The multiply and the add are
//! separate operations on purpose: a fused multiply-add rounds once where the scalar path rounds
//! twice.
//!
//! Two layout details decide the pace of the inner loop, and both were measured before they were
//! adopted (40,000 real rows against 570 centroids, four threads, bit-identical assignments):
//!
//! * The centroids are laid out in dimension tiles: `[d / 32][group of 8 lists][d % 32][lane]`.
//!   A dimension-major array without the tile walks the centroid axis with a stride of
//!   `8 * list_count` bytes, which touches one 64-byte line per 4 KiB page and defeats the
//!   hardware prefetcher; one tile is a contiguous block that the prefetcher follows.
//! * A block of eight rows is converted to `f64` once and read from that buffer. Broadcasting a
//!   converted `f32` costs a conversion and a broadcast on the FP ports, which the three
//!   arithmetic operations per lane already saturate; broadcasting from the staged buffer is a
//!   single load. The conversion is exact and the order is unchanged.
//!
//! Together they took the pass from 7.7e9 to 25.1e9 dimension updates per second on the machine
//! that was measured, without changing a byte of the payload.

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

/// The rows one blocked kernel walks against a centroid group at a time.
const ROWS_PER_BLOCK: usize = 8;

/// The dimensions one tile of the transposed layout covers.
const DIM_TILE: usize = 32;

/// The lanes one centroid group holds. The layout uses eight lanes on every architecture so that
/// the same buffer serves the four-lane and two-lane kernels, which read halves of a group.
const LANES: usize = 8;

/// The centroids, transposed into the tiled layout the kernels read.
///
/// The buffer starts on a 64-byte boundary so an eight-lane load is one aligned load, and lists
/// past `list_count` are zeroed: the lanes they occupy are skipped when the distances are compared
/// but they keep every load inside the allocation.
pub(crate) struct Centroids {
    data: Vec<f64>,
    skip: usize,
    list_count: usize,
    dimension: usize,
    tiles: usize,
    groups: usize,
}

impl Centroids {
    /// Transposes the centroid rows into tiles, padded to whole groups and whole dimension tiles.
    pub(crate) fn build(centroids: &[Vec<f32>], list_count: usize, dimension: usize) -> Self {
        assert!(dimension > 0, "a centroid needs at least one dimension");
        let tiles = dimension.div_ceil(DIM_TILE);
        let groups = list_count.div_ceil(LANES);
        let elements = tiles * groups * DIM_TILE * LANES;
        let mut data = vec![0.0f64; elements + LANES];
        // the allocation is not promised to start on a cache line boundary, so the first aligned
        // element inside it is the one the kernels read from
        let skip = (64 - (data.as_ptr() as usize % 64)) % 64 / size_of::<f64>();
        for group in 0..groups {
            for list in group * LANES..(group * LANES + LANES).min(list_count) {
                for d in 0..dimension {
                    data[skip + offset(groups, d, group, list % LANES)] = centroids[list][d] as f64;
                }
            }
        }

        Self {
            data,
            skip,
            list_count,
            dimension,
            tiles,
            groups,
        }
    }

    fn pointer(&self) -> *const f64 {
        unsafe { self.data.as_ptr().add(self.skip) }
    }

    fn tile_stride(&self) -> usize {
        self.groups * DIM_TILE * LANES
    }

    fn group_stride(&self) -> usize {
        DIM_TILE * LANES
    }

    /// The list's sum from one row, in dimension order: the reference the kernels are held to.
    fn scalar_distance(&self, row: &[f32], list: usize) -> f64 {
        let group = list / LANES;
        let lane = list % LANES;
        let pointer = self.pointer();
        let mut squared = 0.0f64;
        for tile in 0..self.tiles {
            let at = tile * self.tile_stride() + group * self.group_stride() + lane;
            let count = (self.dimension - tile * DIM_TILE).min(DIM_TILE);
            for inner in 0..count {
                let value = unsafe { *pointer.add(at + inner * LANES) };
                let d = tile * DIM_TILE + inner;
                let difference = row[d] as f64 - value;
                squared += difference * difference;
            }
        }

        squared.sqrt()
    }
}

/// The element of the transposed buffer that holds dimension `d` of one list.
fn offset(groups: usize, d: usize, group: usize, lane: usize) -> usize {
    (d / DIM_TILE) * groups * DIM_TILE * LANES
        + group * DIM_TILE * LANES
        + (d % DIM_TILE) * LANES
        + lane
}

/// The distance of one row from every centroid, as the scalar kernel computes it. Test and
/// reference entry point: a kernel that differs from it would move a near-tied row.
#[cfg(test)]
pub(crate) fn distances(row: &[f32], centroids: &Centroids, out: &mut [f64]) {
    assert_eq!(out.len(), centroids.list_count, "one distance per centroid");
    for (list, slot) in out.iter_mut().enumerate() {
        *slot = centroids.scalar_distance(row, list);
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
    let centroids = Centroids::build(centroids, list_count, dimension);
    let width = lanes();
    let threads = threads.max(1).min(rows.len().max(1));
    if threads == 1 {
        nearest_rows(rows, &centroids, width, &mut assignment);
        return assignment;
    }

    let chunk = rows.len().div_ceil(threads);
    std::thread::scope(|scope| {
        for (chunk_index, out) in assignment.chunks_mut(chunk).enumerate() {
            let start = chunk_index * chunk;
            let rows = &rows[start..start + out.len()];
            let centroids = &centroids;
            scope.spawn(move || nearest_rows(rows, centroids, width, out));
        }
    });

    assignment
}

fn nearest_rows(rows: &[&[f32]], centroids: &Centroids, width: usize, out: &mut [usize]) {
    match width {
        #[cfg(target_arch = "x86_64")]
        8 => unsafe { nearest_blocks_avx512(rows, centroids, out) },
        #[cfg(target_arch = "x86_64")]
        4 => unsafe { nearest_blocks_avx2(rows, centroids, out) },
        #[cfg(target_arch = "aarch64")]
        2 => unsafe { nearest_blocks_neon(rows, centroids, out) },
        _ => nearest_scalar(rows, centroids, out),
    }
}

/// The scalar fallback, and the path the last rows of a block go through.
fn nearest_scalar(rows: &[&[f32]], centroids: &Centroids, out: &mut [usize]) {
    for (index, row) in rows.iter().enumerate() {
        out[index] = nearest_of_row(row, centroids);
    }
}

fn nearest_of_row(row: &[f32], centroids: &Centroids) -> usize {
    let mut nearest = 0usize;
    let mut best = f64::INFINITY;
    for list in 0..centroids.list_count {
        let distance = centroids.scalar_distance(row, list);
        if distance < best {
            best = distance;
            nearest = list;
        }
    }

    nearest
}

/// Converts the rows of a block to `f64` once, so the broadcast is a load.
unsafe fn stage_rows(block_rows: &[&[f32]], buffer: &mut [f64], dimension: usize) {
    for (row, source) in block_rows.iter().enumerate() {
        let base = row * dimension;
        for (slot, value) in buffer[base..base + dimension].iter_mut().zip(source.iter()) {
            *slot = *value as f64;
        }
    }
}

// the row index names both the staged row and the accumulator that belongs to it, which iteration
// over one of the two arrays cannot express
#[allow(clippy::needless_range_loop)]
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn nearest_blocks_avx512(rows: &[&[f32]], centroids: &Centroids, out: &mut [usize]) {
    use std::arch::x86_64::*;

    let dimension = centroids.dimension;
    let pointer = centroids.pointer();
    let mut staged = vec![0.0f64; ROWS_PER_BLOCK * dimension];
    let blocks = rows.len() / ROWS_PER_BLOCK;
    for block in 0..blocks {
        let block_rows = rows.get_unchecked(block * ROWS_PER_BLOCK..(block + 1) * ROWS_PER_BLOCK);
        stage_rows(block_rows, &mut staged, dimension);
        let staged_pointer = staged.as_ptr();
        let mut best = [f64::INFINITY; ROWS_PER_BLOCK];
        let mut nearest = [0usize; ROWS_PER_BLOCK];
        for group in 0..centroids.groups {
            let base = group * LANES;
            let mut accumulators = [_mm512_setzero_pd(); ROWS_PER_BLOCK];
            for tile in 0..centroids.tiles {
                let count = (dimension - tile * DIM_TILE).min(DIM_TILE);
                let at =
                    pointer.add(tile * centroids.tile_stride() + group * centroids.group_stride());
                for inner in 0..count {
                    let centroid = _mm512_load_pd(at.add(inner * LANES));
                    let d = tile * DIM_TILE + inner;
                    for row in 0..ROWS_PER_BLOCK {
                        let value = _mm512_set1_pd(*staged_pointer.add(row * dimension + d));
                        let difference = _mm512_sub_pd(value, centroid);
                        accumulators[row] =
                            _mm512_add_pd(accumulators[row], _mm512_mul_pd(difference, difference));
                    }
                }
            }

            for (row, accumulator) in accumulators.iter().enumerate() {
                let mut lanes = [0.0f64; LANES];
                _mm512_storeu_pd(lanes.as_mut_ptr(), *accumulator);
                for (offset, squared) in lanes.iter().enumerate() {
                    let list = base + offset;
                    if list >= centroids.list_count {
                        continue;
                    }

                    let distance = squared.sqrt();
                    if distance < best[row] {
                        best[row] = distance;
                        nearest[row] = list;
                    }
                }
            }
        }

        for (row, nearest_list) in nearest.iter().enumerate() {
            *out.get_unchecked_mut(block * ROWS_PER_BLOCK + row) = *nearest_list;
        }
    }

    let done = blocks * ROWS_PER_BLOCK;
    if done < rows.len() {
        nearest_scalar(
            rows.get_unchecked(done..),
            centroids,
            out.get_unchecked_mut(done..),
        );
    }
}

#[allow(clippy::needless_range_loop)]
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn nearest_blocks_avx2(rows: &[&[f32]], centroids: &Centroids, out: &mut [usize]) {
    use std::arch::x86_64::*;

    let dimension = centroids.dimension;
    let pointer = centroids.pointer();
    let mut staged = vec![0.0f64; ROWS_PER_BLOCK * dimension];
    let blocks = rows.len() / ROWS_PER_BLOCK;
    for block in 0..blocks {
        let block_rows = rows.get_unchecked(block * ROWS_PER_BLOCK..(block + 1) * ROWS_PER_BLOCK);
        stage_rows(block_rows, &mut staged, dimension);
        let staged_pointer = staged.as_ptr();
        let mut best = [f64::INFINITY; ROWS_PER_BLOCK];
        let mut nearest = [0usize; ROWS_PER_BLOCK];
        // an eight-lane group is two four-lane halves, walked one after the other so the lane
        // order of the comparisons stays the scalar one
        for group in 0..centroids.groups {
            let base = group * LANES;
            for half in 0..(LANES / 4) {
                let mut accumulators = [_mm256_setzero_pd(); ROWS_PER_BLOCK];
                for tile in 0..centroids.tiles {
                    let count = (dimension - tile * DIM_TILE).min(DIM_TILE);
                    let at = pointer.add(
                        tile * centroids.tile_stride()
                            + group * centroids.group_stride()
                            + half * 4,
                    );
                    for inner in 0..count {
                        let centroid = _mm256_load_pd(at.add(inner * LANES));
                        let d = tile * DIM_TILE + inner;
                        for row in 0..ROWS_PER_BLOCK {
                            let value = _mm256_set1_pd(*staged_pointer.add(row * dimension + d));
                            let difference = _mm256_sub_pd(value, centroid);
                            accumulators[row] = _mm256_add_pd(
                                accumulators[row],
                                _mm256_mul_pd(difference, difference),
                            );
                        }
                    }
                }

                for (row, accumulator) in accumulators.iter().enumerate() {
                    let mut lanes = [0.0f64; 4];
                    _mm256_storeu_pd(lanes.as_mut_ptr(), *accumulator);
                    for (offset, squared) in lanes.iter().enumerate() {
                        let list = base + half * 4 + offset;
                        if list >= centroids.list_count {
                            continue;
                        }

                        let distance = squared.sqrt();
                        if distance < best[row] {
                            best[row] = distance;
                            nearest[row] = list;
                        }
                    }
                }
            }
        }

        for (row, nearest_list) in nearest.iter().enumerate() {
            *out.get_unchecked_mut(block * ROWS_PER_BLOCK + row) = *nearest_list;
        }
    }

    let done = blocks * ROWS_PER_BLOCK;
    if done < rows.len() {
        nearest_scalar(
            rows.get_unchecked(done..),
            centroids,
            out.get_unchecked_mut(done..),
        );
    }
}

#[allow(clippy::needless_range_loop)]
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn nearest_blocks_neon(rows: &[&[f32]], centroids: &Centroids, out: &mut [usize]) {
    use std::arch::aarch64::*;

    let dimension = centroids.dimension;
    let pointer = centroids.pointer();
    let mut staged = vec![0.0f64; ROWS_PER_BLOCK * dimension];
    let blocks = rows.len() / ROWS_PER_BLOCK;
    for block in 0..blocks {
        let block_rows = rows.get_unchecked(block * ROWS_PER_BLOCK..(block + 1) * ROWS_PER_BLOCK);
        stage_rows(block_rows, &mut staged, dimension);
        let staged_pointer = staged.as_ptr();
        let mut best = [f64::INFINITY; ROWS_PER_BLOCK];
        let mut nearest = [0usize; ROWS_PER_BLOCK];
        // an eight-lane group is four two-lane quarters, walked in lane order
        for group in 0..centroids.groups {
            let base = group * LANES;
            for quarter in 0..(LANES / 2) {
                let mut accumulators = [vdupq_n_f64(0.0); ROWS_PER_BLOCK];
                for tile in 0..centroids.tiles {
                    let count = (dimension - tile * DIM_TILE).min(DIM_TILE);
                    let at = pointer.add(
                        tile * centroids.tile_stride()
                            + group * centroids.group_stride()
                            + quarter * 2,
                    );
                    for inner in 0..count {
                        let centroid = vld1q_f64(at.add(inner * LANES));
                        let d = tile * DIM_TILE + inner;
                        for row in 0..ROWS_PER_BLOCK {
                            let value = vdupq_n_f64(*staged_pointer.add(row * dimension + d));
                            let difference = vsubq_f64(value, centroid);
                            accumulators[row] =
                                vaddq_f64(accumulators[row], vmulq_f64(difference, difference));
                        }
                    }
                }

                for (row, accumulator) in accumulators.iter().enumerate() {
                    let mut lanes = [0.0f64; 2];
                    vst1q_f64(lanes.as_mut_ptr(), *accumulator);
                    for (offset, squared) in lanes.iter().enumerate() {
                        let list = base + quarter * 2 + offset;
                        if list >= centroids.list_count {
                            continue;
                        }

                        let distance = squared.sqrt();
                        if distance < best[row] {
                            best[row] = distance;
                            nearest[row] = list;
                        }
                    }
                }
            }
        }

        for (row, nearest_list) in nearest.iter().enumerate() {
            *out.get_unchecked_mut(block * ROWS_PER_BLOCK + row) = *nearest_list;
        }
    }

    let done = blocks * ROWS_PER_BLOCK;
    if done < rows.len() {
        nearest_scalar(
            rows.get_unchecked(done..),
            centroids,
            out.get_unchecked_mut(done..),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{distances, lanes, nearest, Centroids, ROWS_PER_BLOCK};

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
                let mut best = crate::kmeans::distance(row, &centroids[0]);
                for (list, centroid) in centroids.iter().enumerate().skip(1) {
                    let distance = crate::kmeans::distance(row, centroid);
                    if distance < best {
                        best = distance;
                        nearest = list;
                    }
                }

                nearest
            })
            .collect()
    }

    fn near_tied_rows(count: usize, dimension: usize) -> Vec<Vec<f32>> {
        let mut rows = vectors(count, dimension);
        // rows equidistant from two centroids, and duplicates, are what a changed rounding moves
        for row in rows.iter_mut().take(16) {
            row.fill(0.0);
        }

        rows
    }

    #[test]
    fn the_transposed_layout_sums_like_the_scalar_kernel() {
        let rows = vectors(7, 100);
        let centroids = vectors(19, 100);
        let tiled = Centroids::build(&centroids, centroids.len(), 100);
        let mut expected = vec![0.0f64; centroids.len()];
        for row in &rows {
            distances(row, &tiled, &mut expected);
            for (list, centroid) in centroids.iter().enumerate() {
                let scalar = crate::kmeans::distance(row, centroid);
                assert_eq!(
                    scalar.to_bits(),
                    expected[list].to_bits(),
                    "list {} of row {:?}",
                    list,
                    row
                );
            }
        }
    }

    #[test]
    fn the_assignment_is_the_scalar_one() {
        let mut rows = near_tied_rows(200, 64);
        rows.push(rows[3].clone());
        let mut centroids = vectors(11, 64);
        centroids.push(centroids[2].clone());
        let expected = reference_nearest(&rows, &centroids);
        let row_slices: Vec<&[f32]> = rows.iter().map(|row| row.as_slice()).collect();
        for threads in [1, 2, 4] {
            assert_eq!(
                expected,
                nearest(&row_slices, &centroids, threads),
                "threads {}",
                threads
            );
        }
    }

    #[test]
    fn the_rows_before_between_and_after_full_blocks_match() {
        let centroids = vectors(13, 33);
        for row_count in [0usize, 1, 7, ROWS_PER_BLOCK, ROWS_PER_BLOCK + 1, 17] {
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
        assert!(
            [1, 2, 4, 8].contains(&lanes()),
            "unexpected lane width {}",
            lanes()
        );
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn the_avx512_kernel_keeps_the_scalar_assignment() {
        if !std::arch::is_x86_feature_detected!("avx512f") {
            return;
        }

        let rows = near_tied_rows(40, 77);
        let centroids = vectors(23, 77);
        let tiled = Centroids::build(&centroids, centroids.len(), 77);
        let row_slices: Vec<&[f32]> = rows.iter().map(|row| row.as_slice()).collect();
        let mut assignment = vec![0usize; rows.len()];
        unsafe { super::nearest_blocks_avx512(&row_slices, &tiled, &mut assignment) };
        assert_eq!(reference_nearest(&rows, &centroids), assignment);
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn the_avx2_kernel_keeps_the_scalar_assignment() {
        if !std::arch::is_x86_feature_detected!("avx2") {
            return;
        }

        let rows = near_tied_rows(40, 77);
        let centroids = vectors(23, 77);
        let tiled = Centroids::build(&centroids, centroids.len(), 77);
        let row_slices: Vec<&[f32]> = rows.iter().map(|row| row.as_slice()).collect();
        let mut assignment = vec![0usize; rows.len()];
        unsafe { super::nearest_blocks_avx2(&row_slices, &tiled, &mut assignment) };
        assert_eq!(reference_nearest(&rows, &centroids), assignment);
    }

    #[cfg(target_arch = "aarch64")]
    #[test]
    fn the_neon_kernel_keeps_the_scalar_assignment() {
        if !std::arch::is_aarch64_feature_detected!("neon") {
            return;
        }

        let rows = near_tied_rows(40, 77);
        let centroids = vectors(23, 77);
        let tiled = Centroids::build(&centroids, centroids.len(), 77);
        let row_slices: Vec<&[f32]> = rows.iter().map(|row| row.as_slice()).collect();
        let mut assignment = vec![0usize; rows.len()];
        unsafe { super::nearest_blocks_neon(&row_slices, &tiled, &mut assignment) };
        assert_eq!(reference_nearest(&rows, &centroids), assignment);
    }
}
