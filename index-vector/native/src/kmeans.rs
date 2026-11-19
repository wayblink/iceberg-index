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

//! k-means over one unit's vectors, matching the JVM build's arithmetic.
//!
//! The build is deterministic by construction: the sample is every k-th vector, the starting
//! centroids are points of that sample, and a round stops when it leaves every centroid where it
//! was (or, with a tolerance, when the largest relative move is below it). The distance is the
//! JVM's: `f32` coordinates widened to `f64`, four accumulators over a block of four dimensions,
//! a square root per comparison and ties kept by the earlier list.

use crate::simd;

/// The centroids a round of training produced, and what it spent.
pub struct Training {
    pub centroids: Vec<Vec<f32>>,
    pub iterations: usize,
    pub sample_count: usize,
}

/// How far a centroid moved relative to where it was.
const DISPLACEMENT_EPSILON: f64 = 1e-12;

/// Trains `list_count` centroids on `sample_count` vectors of the unit.
pub fn train(
    vectors: &[Vec<f32>],
    sample_count: usize,
    list_count: usize,
    max_iterations: usize,
    tolerance: f64,
    threads: usize,
) -> Result<Training, String> {
    if vectors.is_empty() {
        return Err("An IVF build needs at least one vector".to_string());
    }

    if list_count == 0 || list_count > vectors.len() {
        return Err(format!(
            "An IVF build of {} vectors cannot have {} lists",
            vectors.len(),
            list_count
        ));
    }

    if sample_count == 0 || sample_count > vectors.len() {
        return Err(format!(
            "An IVF build of {} vectors cannot train on {} samples",
            vectors.len(),
            sample_count
        ));
    }

    let stride = (vectors.len() / sample_count).max(1);
    let sample: Vec<&[f32]> = (0..sample_count)
        .map(|index| {
            let at = (index * stride).min(vectors.len() - 1);
            vectors[at].as_slice()
        })
        .collect();

    let mut centroids: Vec<Vec<f32>> = (0..list_count)
        .map(|list| {
            let at = ((list as u64 * sample_count as u64) / list_count as u64) as usize;
            sample[at.min(sample_count - 1)].to_vec()
        })
        .collect();

    let dimension = centroids[0].len();
    let mut iterations = 0;
    for iteration in 0..max_iterations {
        iterations = iteration + 1;
        let assignment = simd::nearest(&sample, &centroids, threads);
        let mut sums = vec![vec![0.0f32; dimension]; list_count];
        let mut sizes = vec![0usize; list_count];
        for (index, row) in sample.iter().enumerate() {
            let list = assignment[index];
            sizes[list] += 1;
            let sums = &mut sums[list];
            for dimension in 0..dimension {
                sums[dimension] += row[dimension];
            }
        }

        let mut changed = false;
        let mut displacement = 0.0f64;
        for list in 0..list_count {
            if sizes[list] == 0 {
                // a centroid nothing was assigned to keeps its position, as it does on the JVM
                continue;
            }

            let count = sizes[list] as f32;
            let mut mean = vec![0.0f32; dimension];
            for dimension in 0..dimension {
                mean[dimension] = sums[list][dimension] / count;
            }

            if mean
                .iter()
                .zip(centroids[list].iter())
                .all(|(left, right)| left.to_bits() == right.to_bits())
            {
                continue;
            }

            changed = true;
            if tolerance > 0.0 {
                displacement = displacement.max(relative_displacement(&mean, &centroids[list]));
            }
            centroids[list] = mean;
        }

        if !changed {
            break;
        }

        if tolerance > 0.0 && displacement < tolerance {
            break;
        }
    }

    Ok(Training {
        centroids,
        iterations,
        sample_count,
    })
}

/// Puts every vector under its nearest centroid.
pub fn assign(vectors: &[Vec<f32>], centroids: &[Vec<f32>], threads: usize) -> Vec<usize> {
    let rows: Vec<&[f32]> = vectors.iter().map(|vector| vector.as_slice()).collect();
    simd::nearest(&rows, centroids, threads)
}

/// The vectors of every list, in entry order, lists that nothing landed in included.
pub fn members(assignment: &[usize], list_count: usize) -> Vec<Vec<usize>> {
    let mut members = vec![Vec::new(); list_count];
    for (entry, list) in assignment.iter().enumerate() {
        members[*list].push(entry);
    }

    members
}

/// The L2 distance of a row from a centroid, summed the way the JVM sums it.
///
/// The sum is one accumulator in dimension order. The JVM's blocked kernel used to split it into
/// four accumulators for a shorter dependency chain, but that rounds the total differently and a
/// near-tied row can then choose another list, so a rebuild would not reproduce its partitions;
/// the JVM keeps the dimension order and this has to keep it too.
#[cfg(test)]
pub(crate) fn distance(row: &[f32], centroid: &[f32]) -> f64 {
    let dimension = centroid.len();
    let mut squared = 0.0f64;
    for coordinate in 0..dimension {
        let difference = row[coordinate] as f64 - centroid[coordinate] as f64;
        squared += difference * difference;
    }

    squared.sqrt()
}

/// `‖new - old‖ / (‖old‖ + ε)` in `f64`, the JVM's displacement of a centroid.
fn relative_displacement(updated: &[f32], previous: &[f32]) -> f64 {
    let mut moved = 0.0f64;
    let mut before = 0.0f64;
    for dimension in 0..updated.len() {
        let delta = updated[dimension] as f64 - previous[dimension] as f64;
        moved += delta * delta;
        before += (previous[dimension] as f64) * (previous[dimension] as f64);
    }

    moved.sqrt() / (before.sqrt() + DISPLACEMENT_EPSILON)
}

#[cfg(test)]
mod tests {
    use super::{assign, members, train};

    #[test]
    fn keeps_the_earlier_list_on_a_tie() {
        let vectors = vec![vec![0.0f32, 0.0]];
        let centroids = vec![vec![1.0f32, 0.0], vec![-1.0, 0.0]];
        assert_eq!(vec![0], assign(&vectors, &centroids, 1));
    }

    #[test]
    fn splits_every_vector_into_its_list() {
        let vectors = vec![
            vec![0.0f32, 0.0],
            vec![0.1, 0.0],
            vec![10.0, 10.0],
            vec![10.2, 10.0],
        ];
        let trained = train(&vectors, 4, 2, 25, 0.0, 1).expect("training succeeds");
        let assignment = assign(&vectors, &trained.centroids, 1);
        let lists = members(&assignment, 2);
        let mut entries: Vec<usize> = lists.iter().flatten().copied().collect();
        entries.sort_unstable();
        // every entry is in exactly one list, and the two clusters of the sample stay apart
        assert_eq!(vec![0, 1, 2, 3], entries);
        assert_eq!(assignment[0], assignment[1]);
        assert_eq!(assignment[2], assignment[3]);
        assert_ne!(assignment[0], assignment[2]);
    }

    #[test]
    fn stops_after_one_round_when_nothing_moves() {
        let vectors = vec![vec![1.0f32, 2.0], vec![1.0, 2.0], vec![1.0, 2.0]];
        let trained = train(&vectors, 3, 1, 25, 0.0, 1).expect("training succeeds");
        assert_eq!(1, trained.iterations);
        assert_eq!(vec![1.0f32, 2.0], trained.centroids[0]);
    }

    #[test]
    fn threads_do_not_change_the_assignment() {
        let vectors: Vec<Vec<f32>> = (0..64)
            .map(|row| vec![(row as f32) * 0.1, ((row * 7) % 13) as f32])
            .collect();
        let centroids = vec![vec![0.0f32, 0.0], vec![3.0, 3.0], vec![5.0, 1.0]];
        assert_eq!(
            assign(&vectors, &centroids, 1),
            assign(&vectors, &centroids, 4)
        );
    }
}
