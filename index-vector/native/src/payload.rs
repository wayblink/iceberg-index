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

use crate::abi::{self, Writer};

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

/// The exact number of bytes [`write_into`] appends.
///
/// A payload of a large unit is hundreds of megabytes, and letting the destination grow on its own
/// makes it hold the old block and the new one at the same time on every reallocation. Reserving
/// the exact size once is what keeps the write to one block.
pub fn size(spec: &PayloadSpec) -> Result<usize, String> {
    if spec.lists.len() != spec.centroids.len() || spec.list_sizes.len() != spec.centroids.len() {
        return Err(format!(
            "The payload has {} centroids, {} list bodies and {} sizes",
            spec.centroids.len(),
            spec.lists.len(),
            spec.list_sizes.len()
        ));
    }

    let mut total = if spec.list_blocks {
        BLOCK_PREFIX_BYTES
    } else {
        0
    };
    let mut add = |value: usize| {
        total = total
            .checked_add(value)
            .ok_or_else(|| format!("A payload of {} bytes is not addressable", total))?;
        Ok::<(), String>(())
    };

    add(INTEGER_BYTES + abi::modified_utf_len(KERNEL_NAME)? + 2 * INTEGER_BYTES)?;
    if spec.quantized.is_some() {
        // the bits are an int and the seed a long
        add(3 * INTEGER_BYTES)?;
    }

    let centroid_bytes = spec
        .centroids
        .len()
        .checked_mul(spec.dimension)
        .and_then(|coordinates| coordinates.checked_mul(INTEGER_BYTES))
        .ok_or_else(|| "The centroids of this payload overflow".to_string())?;
    add(INTEGER_BYTES + centroid_bytes)?;

    add(INTEGER_BYTES)?;
    for data_file in spec.data_files {
        add(abi::modified_utf_len(data_file)?)?;
    }

    add(INTEGER_BYTES)?;
    if spec.list_blocks {
        add(spec
            .lists
            .len()
            .checked_mul(2 * INTEGER_BYTES)
            .ok_or_else(|| "The list directory of this payload overflows".to_string())?)?;
    }

    for list in spec.lists {
        add(list.len())?;
    }

    Ok(total)
}

/// Writes the payload, appending its bytes to `out`.
pub fn write_into(spec: &PayloadSpec, out: &mut Writer) -> Result<(), String> {
    let total = size(spec)?;
    let start = out.bytes.len();
    out.bytes
        .try_reserve_exact(total)
        .map_err(|error| format!("Cannot hold a payload of {} bytes: {}", total, error))?;

    if spec.list_blocks {
        out.i32(BLOCK_MAGIC);
        out.i32(BLOCK_VERSION);
        // patched below, once the directory has been written
        out.i32(0);
    }

    out.i32(PAYLOAD_HEADER);
    out.modified_utf(KERNEL_NAME)?;
    out.i32(match spec.quantized {
        Some(_) => QUANTIZED_VERSION,
        None => PLAIN_VERSION,
    });
    out.i32(spec.dimension as i32);
    if let Some((bits, seed)) = spec.quantized {
        out.i32(bits as i32);
        out.i64(seed);
    }

    out.i32(spec.centroids.len() as i32);
    for centroid in spec.centroids {
        if centroid.len() != spec.dimension {
            return Err(format!(
                "A centroid of {} dimensions does not fit a payload of {} dimensions",
                centroid.len(),
                spec.dimension
            ));
        }

        for value in centroid {
            out.f32(*value);
        }
    }

    out.i32(spec.data_files.len() as i32);
    for data_file in spec.data_files {
        out.modified_utf(data_file)?;
    }

    out.i32(spec.entry_count as i32);
    let directory = out.bytes.len();
    if spec.list_blocks {
        for _ in 0..spec.lists.len() {
            out.i32(0);
            out.i32(0);
        }
    }

    let metadata_end = out.bytes.len();
    let mut starts = Vec::new();
    starts
        .try_reserve_exact(spec.lists.len())
        .map_err(|error| {
            format!(
                "Cannot hold the directory of {} lists: {}",
                spec.lists.len(),
                error
            )
        })?;
    for list in spec.lists {
        // a payload longer than an int cannot address its lists, and the frame is int-addressed;
        // the directory holds offsets from the start of the payload, which is not the start of the
        // buffer it is being appended to
        let offset = i32::try_from(out.bytes.len() - start).map_err(|_| {
            format!(
                "A payload of {} bytes cannot address its lists",
                out.bytes.len() - start
            )
        })?;
        starts.push(offset);
        out.raw(list);
    }

    if spec.list_blocks {
        let metadata_bytes = metadata_end - start;
        if metadata_bytes > i32::MAX as usize {
            return Err(format!(
                "A payload whose metadata ends at {} bytes is not addressable",
                metadata_bytes
            ));
        }

        out.bytes[start + 2 * INTEGER_BYTES..start + BLOCK_PREFIX_BYTES]
            .copy_from_slice(&(metadata_bytes as i32).to_be_bytes());
        for (index, entry) in out.bytes[directory..metadata_end]
            .chunks_mut(2 * INTEGER_BYTES)
            .enumerate()
        {
            entry[..INTEGER_BYTES].copy_from_slice(&starts[index].to_be_bytes());
            entry[INTEGER_BYTES..].copy_from_slice(&(spec.list_sizes[index] as i32).to_be_bytes());
        }
    }

    debug_assert_eq!(
        out.bytes.len() - start,
        total,
        "the payload size differs from what was reserved"
    );
    Ok(())
}

/// Writes the payload into a new buffer.
pub fn write(spec: &PayloadSpec) -> Result<Vec<u8>, String> {
    let mut payload = Writer::new();
    write_into(spec, &mut payload)?;
    Ok(payload.bytes)
}
