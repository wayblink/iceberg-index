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

//! The build entry point, independent of how the rows are read.
//!
//! The caller names the unit, the counts its definition resolved to, and an [`EntrySource`] that
//! hands the rows over in chunks; this side samples, trains, assigns, encodes and writes the
//! payload. Keeping the row source behind a trait is what lets the same code run against the JNI
//! bridge's chunk callback and against in-memory vectors in tests.

use crate::abi::Writer;
use crate::kmeans;
use crate::payload::{self, PayloadSpec};
use crate::quantizer::{self, Quantizer, FACTORS};
use std::time::Instant;

/// The metric a build assigns with. N0 grows one metric at a time; the L2 path is the one the
/// payloads in use are built by.
pub const METRIC_L2: i32 = 0;

/// How many rows one read asks for. A chunk is small enough to live beside the decoded vectors
/// without a second copy of the unit, and large enough that the per-call overhead of the bridge is
/// not paid per row.
const READ_CHUNK_ROWS: usize = 512;

/// A request to build one payload: the Java side has resolved the definition into counts.
pub struct BuildRequest {
    pub unit_id: String,
    pub dimension: usize,
    pub entry_count: usize,
    pub data_files: Vec<String>,
    pub list_count: usize,
    pub sample_count: usize,
    pub max_iterations: usize,
    pub tolerance: f64,
    /// Bits per dimension of the RaBitQ code, or zero when the vectors are stored as themselves.
    pub bits: u8,
    pub seed: i64,
    pub metric: i32,
    pub list_blocks: bool,
    pub threads: usize,
}

/// The rows of a unit, read in chunks by the caller's own storage layer.
pub trait EntrySource {
    /// Fills `out` with `count` records, each a data file index, a row position and a float vector.
    fn fetch(&mut self, start: usize, count: usize, out: &mut Vec<u8>) -> Result<(), String>;
}

/// What a build produced.
#[derive(Debug)]
pub struct BuildOutcome {
    pub payload: Vec<u8>,
    pub iterations: usize,
    pub sample_count: usize,
    pub nanos_read: u64,
    pub nanos_train: u64,
    pub nanos_assign: u64,
    pub nanos_encode: u64,
}

/// Builds one IVF payload.
pub fn build(request: &BuildRequest, source: &mut dyn EntrySource) -> Result<BuildOutcome, String> {
    let unit = request.unit_id.as_str();
    build_inner(request, source).map_err(|message| {
        if unit.is_empty() {
            message
        } else {
            format!("Index unit {}: {}", unit, message)
        }
    })
}

fn build_inner(
    request: &BuildRequest,
    source: &mut dyn EntrySource,
) -> Result<BuildOutcome, String> {
    validate(request)?;

    let read_start = Instant::now();
    let mut vectors: Vec<Vec<f32>> = Vec::with_capacity(request.entry_count);
    let mut positions: Vec<(i32, i64)> = Vec::with_capacity(request.entry_count);
    let record_bytes = record_bytes(request.dimension);
    let mut buffer = Vec::new();
    let mut read = 0;
    while read < request.entry_count {
        let count = READ_CHUNK_ROWS.min(request.entry_count - read);
        buffer.clear();
        source.fetch(read, count, &mut buffer)?;
        if buffer.len() != count * record_bytes {
            return Err(format!(
                "The row source handed {} bytes for {} rows of {} dimensions; {} were needed",
                buffer.len(),
                count,
                request.dimension,
                count * record_bytes
            ));
        }

        let mut reader = crate::abi::Reader::new(&buffer);
        for _ in 0..count {
            let data_file = reader.i32("data file index")?;
            if data_file < 0 || data_file as usize >= request.data_files.len() {
                return Err(format!(
                    "A row names data file {}, but the unit has {} files",
                    data_file,
                    request.data_files.len()
                ));
            }

            let row_position = reader.i64("row position")?;
            let mut vector = Vec::with_capacity(request.dimension);
            for _ in 0..request.dimension {
                let value = reader.f32("vector coordinate")?;
                if !value.is_finite() {
                    return Err(format!(
                        "A row of data file {} position {} holds a value that is not finite: {}",
                        data_file, row_position, value
                    ));
                }

                vector.push(value);
            }

            positions.push((data_file, row_position));
            vectors.push(vector);
        }

        read += count;
    }
    let nanos_read = nanos(read_start);

    let train_start = Instant::now();
    let trained = kmeans::train(
        &vectors,
        request.sample_count,
        request.list_count,
        request.max_iterations,
        request.tolerance,
        request.threads,
    )?;
    let nanos_train = nanos(train_start);

    let assign_start = Instant::now();
    let assignment = kmeans::assign(&vectors, &trained.centroids, request.threads);
    let members = kmeans::members(&assignment, request.list_count);
    let nanos_assign = nanos(assign_start);

    let encode_start = Instant::now();
    let encoded = encode_lists(request, &vectors, &positions, &members)?;
    let nanos_encode = nanos(encode_start);

    let mut lists = Vec::with_capacity(encoded.len());
    let mut list_sizes = Vec::with_capacity(encoded.len());
    for (body, size) in encoded {
        lists.push(body);
        list_sizes.push(size);
    }

    let payload = payload::write(&PayloadSpec {
        dimension: request.dimension,
        centroids: &trained.centroids,
        data_files: &request.data_files,
        entry_count: request.entry_count,
        quantized: if request.bits == 0 {
            None
        } else {
            Some((request.bits, request.seed))
        },
        list_blocks: request.list_blocks,
        lists: &lists,
        list_sizes: &list_sizes,
    })?;

    Ok(BuildOutcome {
        payload,
        iterations: trained.iterations,
        sample_count: trained.sample_count,
        nanos_read,
        nanos_train,
        nanos_assign,
        nanos_encode,
    })
}

/// The bytes of every list and its member count, in centroid order.
fn encode_lists(
    request: &BuildRequest,
    vectors: &[Vec<f32>],
    positions: &[(i32, i64)],
    members: &[Vec<usize>],
) -> Result<Vec<(Vec<u8>, usize)>, String> {
    let dimensions = request.dimension;
    let signs = if request.bits == 0 {
        Vec::new()
    } else {
        quantizer::signs(request.seed, quantizer::next_power_of_two(dimensions)?)
    };

    let threads = request.threads.max(1).min(members.len().max(1));
    let mut encoded: Vec<Option<EncodedList>> = (0..members.len()).map(|_| None).collect();
    if threads == 1 {
        for (list, slot) in encoded.iter_mut().enumerate() {
            *slot = Some(encode_list(
                list,
                request,
                vectors,
                positions,
                &members[list],
                &signs,
            ));
        }
    } else {
        let chunk = members.len().div_ceil(threads);
        let signs = &signs;
        std::thread::scope(|scope| {
            for (chunk_index, out) in encoded.chunks_mut(chunk).enumerate() {
                let start = chunk_index * chunk;
                scope.spawn(move || {
                    for (offset, slot) in out.iter_mut().enumerate() {
                        let list = start + offset;
                        *slot = Some(encode_list(
                            list,
                            request,
                            vectors,
                            positions,
                            &members[list],
                            signs,
                        ));
                    }
                });
            }
        });
    }

    encoded
        .into_iter()
        .map(|slot| slot.expect("every list is encoded"))
        .collect()
}

/// One encoded list body and the number of entries it holds.
type EncodedList = Result<(Vec<u8>, usize), String>;

/// Encodes one list: the vectors themselves, or one RaBitQ code and its factors per entry.
fn encode_list(
    list: usize,
    request: &BuildRequest,
    vectors: &[Vec<f32>],
    positions: &[(i32, i64)],
    members: &[usize],
    signs: &[f32],
) -> Result<(Vec<u8>, usize), String> {
    let mut body = Writer::new();
    if request.bits == 0 {
        body.i32(members.len() as i32);
        for member in members {
            body.i32(positions[*member].0);
            body.i64(positions[*member].1);
            for value in &vectors[*member] {
                body.f32(*value);
            }
        }

        return Ok((body.bytes, members.len()));
    }

    let selected: Vec<&[f32]> = members
        .iter()
        .map(|member| vectors[*member].as_slice())
        .collect();
    let quantizer = Quantizer::train(&selected, request.dimension, request.bits)
        .map_err(|message| format!("List {}: {}", list, message))?;
    for value in quantizer.center() {
        body.f32(*value);
    }

    body.i32(members.len() as i32);
    let mut code = vec![0u8; quantizer.code_bytes()];
    let mut factors = [0.0f32; FACTORS];
    for member in members {
        quantizer
            .encode(signs, &vectors[*member], &mut code, &mut factors)
            .map_err(|message| format!("List {}: {}", list, message))?;
        body.i32(positions[*member].0);
        body.i64(positions[*member].1);
        body.raw(&code);
        for factor in factors {
            body.f32(factor);
        }
    }

    Ok((body.bytes, members.len()))
}

fn validate(request: &BuildRequest) -> Result<(), String> {
    if request.metric != METRIC_L2 {
        return Err(format!(
            "The native IVF build kernel supports the L2 metric only: metric {}",
            request.metric
        ));
    }

    if request.dimension == 0 {
        return Err("The index dimension must be positive: 0".to_string());
    }

    if request.entry_count == 0 {
        return Err("An IVF build needs at least one vector".to_string());
    }

    if request.entry_count > i32::MAX as usize {
        return Err(format!(
            "An IVF build of {} entries is not addressable",
            request.entry_count
        ));
    }

    if request.list_count == 0 || request.list_count > request.entry_count {
        return Err(format!(
            "An IVF build of {} entries cannot have {} lists",
            request.entry_count, request.list_count
        ));
    }

    if request.sample_count == 0 || request.sample_count > request.entry_count {
        return Err(format!(
            "An IVF build of {} entries cannot train on {} samples",
            request.entry_count, request.sample_count
        ));
    }

    if request.max_iterations == 0 {
        return Err("An IVF build runs at least one training round: 0".to_string());
    }

    if !request.tolerance.is_finite() || request.tolerance < 0.0 || request.tolerance > 0.1 {
        return Err(format!(
            "The training tolerance must be between 0 and 0.1: {}",
            request.tolerance
        ));
    }

    if request.bits > 8 {
        return Err(format!(
            "Bits per dimension must be at most 8: {}",
            request.bits
        ));
    }

    if request.threads == 0 {
        return Err("An IVF build needs at least one thread: 0".to_string());
    }

    Ok(())
}

fn nanos(started: Instant) -> u64 {
    started.elapsed().as_nanos() as u64
}

/// The bytes one row of the chunk protocol takes: a data file index, a row position and a vector.
pub fn record_bytes(dimension: usize) -> usize {
    INTEGER_BYTES + 8 + dimension * 4
}

const INTEGER_BYTES: usize = 4;

#[cfg(test)]
mod tests {
    use super::{build, record_bytes, BuildRequest, EntrySource, METRIC_L2};
    use crate::abi::Reader;

    struct MemorySource {
        vectors: Vec<Vec<f32>>,
        data_files: Vec<i32>,
    }

    impl EntrySource for MemorySource {
        fn fetch(&mut self, start: usize, count: usize, out: &mut Vec<u8>) -> Result<(), String> {
            let dimension = self.vectors[start].len();
            out.clear();
            for index in start..start + count {
                out.extend_from_slice(&self.data_files[index].to_be_bytes());
                out.extend_from_slice(&(index as i64).to_be_bytes());
                for value in &self.vectors[index] {
                    out.extend_from_slice(&value.to_bits().to_be_bytes());
                }
            }

            assert_eq!(count * record_bytes(dimension), out.len());
            Ok(())
        }
    }

    fn vectors(count: usize, dimension: usize) -> Vec<Vec<f32>> {
        (0..count)
            .map(|row| {
                (0..dimension)
                    .map(|column| (((row * 37 + column * 101) % 1000) as f32 / 1000.0 - 0.5) * 4.0)
                    .collect()
            })
            .collect()
    }

    fn request(bits: u8, list_blocks: bool, dimension: usize, count: usize) -> BuildRequest {
        BuildRequest {
            unit_id: "unit-0".to_string(),
            dimension,
            entry_count: count,
            data_files: vec!["data-0.parquet".to_string(), "data-1.parquet".to_string()],
            list_count: 7,
            sample_count: count.min(7 * 39),
            max_iterations: 25,
            tolerance: 0.0,
            bits,
            seed: 0x9E37_79B9_7F4A_7C15u64 as i64,
            metric: METRIC_L2,
            list_blocks,
            threads: 2,
        }
    }

    fn source(count: usize, dimension: usize) -> MemorySource {
        MemorySource {
            vectors: vectors(count, dimension),
            data_files: (0..count).map(|index| (index / 50) as i32).collect(),
        }
    }

    #[test]
    fn builds_a_plain_payload() {
        let mut request = request(0, true, 17, 100);
        request.data_files = vec!["data-0.parquet".to_string(), "data-1.parquet".to_string()];
        let outcome = build(&request, &mut source(100, 17)).expect("the plain payload builds");
        assert_eq!(
            crate::payload::BLOCK_MAGIC,
            i32::from_be_bytes([
                outcome.payload[0],
                outcome.payload[1],
                outcome.payload[2],
                outcome.payload[3]
            ])
        );
        assert!(outcome.sample_count <= 100);
        assert!(outcome.iterations >= 1);
    }

    #[test]
    fn builds_a_quantized_payload() {
        let request = request(2, false, 17, 100);
        let outcome = build(&request, &mut source(100, 17)).expect("the quantized payload builds");
        assert!(!outcome.payload.is_empty());
        let mut reader = Reader::new(&outcome.payload);
        assert_eq!(3, reader.i32("header").unwrap());
        assert_eq!("ivf", reader.modified_utf("kernel").unwrap());
        assert_eq!(
            crate::payload::QUANTIZED_VERSION,
            reader.i32("kernel version").unwrap()
        );
        assert_eq!(17, reader.i32("dimension").unwrap());
        assert_eq!(2, reader.i32("bits").unwrap());
    }

    #[test]
    fn refuses_a_metric_other_than_l2() {
        let mut request = request(0, false, 17, 100);
        request.metric = 1;
        let error = build(&request, &mut source(100, 17)).expect_err("cosine is not supported");
        assert!(error.contains("L2"), "{}", error);
    }

    #[test]
    fn threads_do_not_change_the_payload() {
        let mut serial = request(0, true, 17, 100);
        serial.threads = 1;
        let mut parallel = request(0, true, 17, 100);
        parallel.threads = 4;
        assert_eq!(
            build(&serial, &mut source(100, 17)).unwrap().payload,
            build(&parallel, &mut source(100, 17)).unwrap().payload
        );
    }
}
