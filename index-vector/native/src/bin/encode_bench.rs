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

//! Throughput of [`Quantizer::encode`] on one list of real vectors.
//!
//! Not product code: this binary exists so that an optimization of the encoder can be measured on
//! the same inputs, in the same process shape, as the one it replaces. It reads `rows` vectors of
//! `dimension` little-endian `f32` from a raw file, trains one quantizer on the first `train` of
//! them, and encodes that many vectors `rounds` times, reporting the best round.
//!
//! ```text
//! cargo run --release --bin encode_bench -- <vectors.f32> <rows> <dimension> <bits> <rounds>
//! ```
//!
//! A single thread encodes, which is what one build thread of the kernel does; the process is
//! otherwise idle. The checksum is printed so that two builds of the encoder can be compared for
//! identical output, and so that the loop cannot be optimized away.

use std::io::Read;
use std::time::Instant;

use iceberg_ivf_native::quantizer::{self, Quantizer, FACTORS};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 6 {
        eprintln!("usage: encode_bench <vectors.f32> <rows> <dimension> <bits> <rounds> [train]");
        std::process::exit(2);
    }

    let path = &args[1];
    let rows: usize = args[2].parse().expect("rows");
    let dimension: usize = args[3].parse().expect("dimension");
    let bits: u8 = args[4].parse().expect("bits");
    let rounds: usize = args[5].parse().expect("rounds");
    let train: usize = args
        .get(6)
        .map(|value| value.parse().expect("train"))
        .unwrap_or(2000)
        .min(rows);

    let mut file = std::fs::File::open(path).expect("open input");
    let mut raw = vec![0u8; rows * dimension * 4];
    file.read_exact(&mut raw).expect("read input");
    let mut vectors: Vec<Vec<f32>> = Vec::with_capacity(rows);
    for row in 0..rows {
        let mut vector = Vec::with_capacity(dimension);
        for d in 0..dimension {
            let offset = (row * dimension + d) * 4;
            vector.push(f32::from_le_bytes([
                raw[offset],
                raw[offset + 1],
                raw[offset + 2],
                raw[offset + 3],
            ]));
        }
        vectors.push(vector);
    }

    let selected: Vec<&[f32]> = vectors[..train].iter().map(|v| v.as_slice()).collect();
    let quantizer = Quantizer::train(&selected, dimension, bits).expect("train");
    let signs = quantizer::signs(0x9E3779B97F4A7C15u64 as i64, quantizer.padded());
    let mut code = vec![0u8; quantizer.code_bytes()];
    let mut factors = [0.0f32; FACTORS];

    let mut checksum = 0xcbf29ce484222325u64;
    let mut best = f64::MAX;
    for round in 0..rounds {
        let started = Instant::now();
        for vector in &vectors[..train] {
            quantizer
                .encode(&signs, vector, &mut code, &mut factors)
                .expect("encode");
            let first = u64::from_le_bytes(code[..8].try_into().unwrap());
            let last = u64::from_le_bytes(code[code.len() - 8..].try_into().unwrap());
            checksum = (checksum ^ first)
                .wrapping_mul(0x100000001b3)
                .wrapping_add(last ^ factors[1].to_bits() as u64);
        }
        let elapsed = started.elapsed().as_secs_f64();
        best = best.min(elapsed);
        println!(
            "round {}: {:.3} s total, {:.2} us/vector",
            round,
            elapsed,
            elapsed / train as f64 * 1e6
        );
    }

    println!(
        "arch {} dimension {} padded {} bits {} vectors {} best {:.2} us/vector checksum {:016x}",
        std::env::consts::ARCH,
        dimension,
        quantizer.padded(),
        bits,
        train,
        best / train as f64 * 1e6,
        checksum
    );
}
