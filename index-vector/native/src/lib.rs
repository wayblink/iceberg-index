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

//! Native IVF index build kernel.
//!
//! The crate is both the JNI library the Java build loads and the library its own tests run
//! against; the JNI surface is a thin frame codec in front of [`build`].

pub mod abi;
pub mod build;
pub mod java_random;
mod jni_bridge;
pub mod kmeans;
pub mod payload;
pub mod quantizer;
pub mod simd;

pub use build::{build, BuildOutcome, BuildRequest, EntrySource};

/// The version of the wire format between the Java bridge and this library.
pub const ABI_VERSION: i32 = 2;
