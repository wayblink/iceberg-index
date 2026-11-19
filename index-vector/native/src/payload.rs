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

//! The IVF payload the Java reader loads, written byte for byte like the JVM's writer.
//!
//! A payload is a header naming the kernel, the dimension (and the quantizer's bits and seed when
//! the vectors are stored as codes), the centroids, the dictionary of data files, the entry count,
//! an optional list directory, and the lists themselves in centroid order. The block form puts a
//! three-int prefix in front and fills the directory with each list's offset and member count.

use crate::abi::Writer;

/// The kernel name every IVF payload carries.
pub const KERNEL_NAME: &str = "ivf";

/// The header that names the kernel and its own format version.
const PAYLOAD_HEADER: i32 = 3;

/// The plain payload, which stores the vectors themselves.
pub const PLAIN_VERSION: i32 = 1;

/// The payload written when the vectors are quantized.
pub const QUANTIZED_VERSION: i32 = 2;

/// The prefix of the block form: magic, block version, and the offset the metadata ends at.
pub const BLOCK_MAGIC: i32 = 0x49564642;
pub const BLOCK_VERSION: i32 = 1;
const BLOCK_PREFIX_BYTES: usize = 3 * INTEGER_BYTES;

const INTEGER_BYTES: usize = 4;

/// Everything the writer needs besides the lists' bytes.
pub struct PayloadSpec<'a> {
    pub dimension: usize,
    pub centroids: &'a [Vec<f32>],
    pub data_files: &'a [String],
    pub entry_count: usize,
    pub quantized: Option<(u8, i64)>,
    pub list_blocks: bool,
    /// One encoded list body per centroid, in centroid order.
    pub lists: &'a [Vec<u8>],
    /// How many entries each encoded list body holds.
    pub list_sizes: &'a [usize],
}

/// Writes the payload.
pub fn write(spec: &PayloadSpec) -> Result<Vec<u8>, String> {
    if spec.lists.len() != spec.centroids.len() || spec.list_sizes.len() != spec.centroids.len() {
        return Err(format!(
            "The payload has {} centroids, {} list bodies and {} sizes",
            spec.centroids.len(),
            spec.lists.len(),
            spec.list_sizes.len()
        ));
    }

    let mut payload = Writer::new();
    if spec.list_blocks {
        payload.i32(BLOCK_MAGIC);
        payload.i32(BLOCK_VERSION);
        // patched below, once the directory has been written
        payload.i32(0);
    }

    payload.i32(PAYLOAD_HEADER);
    payload.modified_utf(KERNEL_NAME)?;
    payload.i32(match spec.quantized {
        Some(_) => QUANTIZED_VERSION,
        None => PLAIN_VERSION,
    });
    payload.i32(spec.dimension as i32);
    if let Some((bits, seed)) = spec.quantized {
        payload.i32(bits as i32);
        payload.i64(seed);
    }

    payload.i32(spec.centroids.len() as i32);
    for centroid in spec.centroids {
        if centroid.len() != spec.dimension {
            return Err(format!(
                "A centroid of {} dimensions does not fit a payload of {} dimensions",
                centroid.len(),
                spec.dimension
            ));
        }

        for value in centroid {
            payload.f32(*value);
        }
    }

    payload.i32(spec.data_files.len() as i32);
    for data_file in spec.data_files {
        payload.modified_utf(data_file)?;
    }

    payload.i32(spec.entry_count as i32);
    let directory = payload.bytes.len();
    if spec.list_blocks {
        for _ in 0..spec.lists.len() {
            payload.i32(0);
            payload.i32(0);
        }
    }

    let metadata_end = payload.bytes.len();
    let mut starts = Vec::with_capacity(spec.lists.len());
    for list in spec.lists {
        starts.push(payload.bytes.len() as i32);
        payload.raw(list);
    }

    if spec.list_blocks {
        if metadata_end > i32::MAX as usize {
            return Err(format!(
                "A payload whose metadata ends at {} bytes is not addressable",
                metadata_end
            ));
        }

        payload.bytes[2 * INTEGER_BYTES..BLOCK_PREFIX_BYTES]
            .copy_from_slice(&(metadata_end as i32).to_be_bytes());
        for (index, out) in payload.bytes[directory..metadata_end]
            .chunks_mut(2 * INTEGER_BYTES)
            .enumerate()
        {
            out[..INTEGER_BYTES].copy_from_slice(&starts[index].to_be_bytes());
            out[INTEGER_BYTES..].copy_from_slice(&(spec.list_sizes[index] as i32).to_be_bytes());
        }
    }

    Ok(payload.bytes)
}
