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

//! The JNI surface: one open/build/close context per unit build.
//!
//! Java hands the request as a frame of bytes and an object whose `fetch` fills a direct buffer
//! with rows, so the rows never become one array on the Java side. The build runs on the calling
//! thread and asks for chunks through that object; only assignment and encoding are parallel, and
//! they write disjoint slots, so the payload does not depend on the thread count.

use crate::abi::{Reader, Writer};
use crate::build::{build, BuildOutcome, BuildRequest, EntrySource, METRIC_L2};
use crate::ABI_VERSION;
use jni::errors::Error as JniError;
use jni::objects::{GlobalRef, JByteArray, JClass, JObject, JString, JValue};
use jni::sys::{jbyteArray, jint, jlong};
use jni::{JNIEnv, JavaVM};
use std::panic::{catch_unwind, AssertUnwindSafe};

const PARAMS_MAGIC: i32 = 0x4E495650;
const RESULT_MAGIC: i32 = 0x4E495652;

/// The state one `nativeOpen` allocation carries between the native calls.
struct NativeContext {
    request: BuildRequest,
    source: GlobalRef,
    vm: JavaVM,
}

/// The JNI native methods of `org.apache.iceberg.index.vector.NativeIvfBuild`.
#[no_mangle]
pub extern "system" fn Java_org_apache_iceberg_index_vector_NativeIvfBuild_nativeVersion<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jint {
    ABI_VERSION
}

#[no_mangle]
pub extern "system" fn Java_org_apache_iceberg_index_vector_NativeIvfBuild_nativeOpen<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    params: JByteArray<'local>,
    source: JObject<'local>,
) -> jlong {
    let opened = catch_unwind(AssertUnwindSafe(|| open(&mut env, params, source)));
    match opened {
        Ok(Ok(handle)) => handle,
        Ok(Err(message)) => {
            throw(&mut env, "java/lang/IllegalArgumentException", &message);
            0
        }
        Err(_) => {
            throw(
                &mut env,
                "java/lang/IllegalStateException",
                "The native IVF kernel panicked while reading its build request",
            );
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_apache_iceberg_index_vector_NativeIvfBuild_nativeBuild<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyteArray {
    let built = catch_unwind(AssertUnwindSafe(|| build_native(handle)));
    match built {
        Ok(Ok(frame)) => match env.byte_array_from_slice(&frame) {
            Ok(array) => array.into_raw(),
            Err(error) => {
                throw(
                    &mut env,
                    "java/lang/IllegalStateException",
                    &format!(
                        "The native IVF kernel could not return its payload: {}",
                        error
                    ),
                );
                JObject::null().into_raw() as jbyteArray
            }
        },
        Ok(Err(message)) => {
            clear_pending(&mut env);
            throw(&mut env, "java/lang/IllegalStateException", &message);
            JObject::null().into_raw() as jbyteArray
        }
        Err(_) => {
            clear_pending(&mut env);
            throw(
                &mut env,
                "java/lang/IllegalStateException",
                "The native IVF kernel panicked while building a payload",
            );
            JObject::null().into_raw() as jbyteArray
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_apache_iceberg_index_vector_NativeIvfBuild_nativeClose<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    if handle == 0 {
        return;
    }

    // SAFETY: a non-zero handle is the pointer `nativeOpen` handed out and has not been closed: the
    // Java bridge closes each context once, in a finally block.
    unsafe {
        drop(Box::from_raw(handle as *mut NativeContext));
    }
}

fn open(env: &mut JNIEnv, params: JByteArray, source: JObject) -> Result<jlong, String> {
    let bytes = env
        .convert_byte_array(&params)
        .map_err(|error| format!("Cannot read the build request bytes: {}", error))?;
    let request = decode_params(&bytes)?;
    if source.is_null() {
        return Err("The row source of an IVF build is null".to_string());
    }

    let count = env
        .call_method(&source, "count", "()I", &[])
        .and_then(|value| value.i())
        .map_err(|error| format!("Cannot ask the row source for its size: {}", error))?;
    if count < 0 || count as usize != request.entry_count {
        return Err(format!(
            "The row source holds {} rows but the build request names {}",
            count, request.entry_count
        ));
    }

    let source = env
        .new_global_ref(source)
        .map_err(|error| format!("Cannot keep the row source alive: {}", error))?;
    let vm = env
        .get_java_vm()
        .map_err(|error| format!("Cannot reach the Java VM: {}", error))?;
    let context = Box::new(NativeContext {
        request,
        source,
        vm,
    });
    Ok(Box::into_raw(context) as jlong)
}

fn build_native(handle: jlong) -> Result<Vec<u8>, String> {
    if handle == 0 {
        return Err("The native build context is null".to_string());
    }

    // SAFETY: the pointer is the one `nativeOpen` returned and `nativeClose` has not run yet.
    let context = unsafe { &*(handle as *const NativeContext) };
    let mut guard = context
        .vm
        .attach_current_thread()
        .map_err(|error| format!("Cannot attach the build thread to the JVM: {}", error))?;
    let record_bytes = crate::build::record_bytes(context.request.dimension);
    let mut source = JniSource {
        env: &mut guard,
        source: &context.source,
        record_bytes,
    };
    let outcome = build(&context.request, &mut source)?;
    encode_result(&context.request, &outcome)
}

/// Reads the rows of one unit through the Java object's `fetch`.
struct JniSource<'a, 'local> {
    env: &'a mut JNIEnv<'local>,
    source: &'a GlobalRef,
    record_bytes: usize,
}

impl EntrySource for JniSource<'_, '_> {
    fn fetch(&mut self, start: usize, count: usize, out: &mut Vec<u8>) -> Result<(), String> {
        let bytes = count
            .checked_mul(self.record_bytes)
            .ok_or_else(|| format!("Reading {} rows overflows the chunk size", count))?;
        out.resize(bytes, 0);
        // SAFETY: the buffer outlives the call below; the row source fills it and must not keep the
        // ByteBuffer, which the Java bridge's contract states.
        let buffer = unsafe { self.env.new_direct_byte_buffer(out.as_mut_ptr(), out.len()) }
            .map_err(|error| format!("Cannot expose the row buffer to Java: {}", error))?;
        let buffer = JObject::from(buffer);
        let arguments = [
            JValue::Int(start as jint),
            JValue::Int(count as jint),
            JValue::Object(&buffer),
        ];
        let fetched = self.env.call_method(
            self.source.as_obj(),
            "fetch",
            "(IILjava/nio/ByteBuffer;)V",
            &arguments,
        );
        match fetched {
            Ok(_) => Ok(()),
            Err(JniError::JavaException) => Err(format!(
                "Reading rows {}..{} failed: {}",
                start,
                start + count,
                take_exception(self.env)
            )),
            Err(error) => Err(format!(
                "Reading rows {}..{} failed: {}",
                start,
                start + count,
                error
            )),
        }
    }
}

fn decode_params(bytes: &[u8]) -> Result<BuildRequest, String> {
    let mut reader = Reader::new(bytes);
    let magic = reader.i32("request magic")?;
    if magic != PARAMS_MAGIC {
        return Err(format!(
            "The build request starts with 0x{:08x}, not the IVF params magic",
            magic as u32
        ));
    }

    let version = reader.i32("request version")?;
    if version != ABI_VERSION {
        return Err(format!(
            "The build request is version {} but this kernel reads version {}",
            version, ABI_VERSION
        ));
    }

    let unit_id = reader.modified_utf("unit id")?;
    let dimension = count(reader.i32("dimension")?, "dimension")?;
    let entry_count = count(reader.i32("entry count")?, "entry count")?;
    let data_file_count = count(reader.i32("data file count")?, "data file count")?;
    let mut data_files = Vec::with_capacity(data_file_count);
    for _ in 0..data_file_count {
        data_files.push(reader.modified_utf("data file path")?);
    }

    let list_count = count(reader.i32("list count")?, "list count")?;
    let sample_count = count(reader.i32("sample count")?, "sample count")?;
    let max_iterations = count(reader.i32("max iterations")?, "max iterations")?;
    let tolerance = f64::from_bits(reader.i64("tolerance")? as u64);
    let bits = reader.i32("bits")?;
    if !(0..=8).contains(&bits) {
        return Err(format!(
            "Bits per dimension must be 0 to 8, where zero means the vectors are not quantized: {}",
            bits
        ));
    }

    let seed = reader.i64("quantizer seed")?;
    let metric = reader.i32("metric")?;
    if metric != METRIC_L2 {
        return Err(format!(
            "The native IVF build kernel supports the L2 metric only: metric {}",
            metric
        ));
    }

    let list_blocks = reader.boolean("list blocks")?;
    let threads = count(reader.i32("threads")?, "threads")?;
    reader.finished("build request")?;
    Ok(BuildRequest {
        unit_id,
        dimension,
        entry_count,
        data_files,
        list_count,
        sample_count,
        max_iterations,
        tolerance,
        bits: bits as u8,
        seed,
        metric,
        list_blocks,
        threads,
    })
}

fn encode_result(request: &BuildRequest, outcome: &BuildOutcome) -> Result<Vec<u8>, String> {
    let payload_bytes = i32::try_from(outcome.payload.len()).map_err(|_| {
        format!(
            "A payload of {} bytes is not addressable",
            outcome.payload.len()
        )
    })?;
    let mut frame = Writer::new();
    frame.i32(RESULT_MAGIC);
    frame.i32(ABI_VERSION);
    frame.i32(0);
    frame.modified_utf("")?;
    frame.i32(request.list_count as i32);
    frame.i32(outcome.sample_count as i32);
    frame.i32(outcome.iterations as i32);
    frame.i64(request.entry_count as i64);
    frame.i64(outcome.nanos_read as i64);
    frame.i64(outcome.nanos_train as i64);
    frame.i64(outcome.nanos_assign as i64);
    frame.i64(outcome.nanos_encode as i64);
    frame.i32(payload_bytes);
    frame.raw(&outcome.payload);
    Ok(frame.bytes)
}

fn count(value: i32, what: &str) -> Result<usize, String> {
    if value < 0 {
        return Err(format!(
            "The build request has a negative {}: {}",
            what, value
        ));
    }

    Ok(value as usize)
}

fn throw(env: &mut JNIEnv, class: &str, message: &str) {
    let _ = env.throw_new(class, message);
}

/// Takes the pending Java exception, so a later JNI call is legal, and reads what it said.
fn take_exception(env: &mut JNIEnv) -> String {
    if !env.exception_check().unwrap_or(false) {
        return "a Java exception that was not reported".to_string();
    }

    let throwable = match env.exception_occurred() {
        Ok(throwable) => throwable,
        Err(_) => {
            let _ = env.exception_clear();
            return "a Java exception that could not be read".to_string();
        }
    };
    // the throwable has to be readable before a method is called on it, so the pending state goes
    // first; the local reference keeps the object alive
    let _ = env.exception_clear();
    let mut text = None;
    if let Ok(value) = env.call_method(&throwable, "toString", "()Ljava/lang/String;", &[]) {
        if let Ok(object) = value.l() {
            let string = JString::from(object);
            if let Ok(java_str) = env.get_string(&string) {
                text = Some(String::from(java_str));
            };
        }
    }

    text.unwrap_or_else(|| "a Java exception with no message".to_string())
}

fn clear_pending(env: &mut JNIEnv) {
    if env.exception_check().unwrap_or(false) {
        let _ = take_exception(env);
    }
}
