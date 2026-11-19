# Native IVF build kernel

The Rust side of the native IVF build kernel: it takes one unit's vectors, trains the centroids,
assigns the vectors, quantizes them (RaBitQ) and writes the payload the Java reader loads. The
crate is both the JNI library the JVM loads and a library its own tests run against.

This is the N0 prototype of #3144. It is not wired into a `VectorKernel` and has no user-facing
option; the only caller is `NativeIvfBuild` in `index-vector`, which the golden test drives.

## Layout

| Path | Contents |
|---|---|
| `src/abi.rs` | The frame codec the bridge uses: big-endian numbers and Java's modified UTF-8 |
| `src/java_random.rs` | `java.util.Random`, reproduced for the quantizer's rotation signs |
| `src/quantizer.rs` | The RaBitQ code of one list |
| `src/kmeans.rs` | Sampling, training and assignment, on the arithmetic the JVM build uses |
| `src/payload.rs` | The payload layout, including the block form's directory |
| `src/build.rs` | The entry point: read chunks, train, assign, encode, write |
| `src/jni_bridge.rs` | The JNI surface (`NativeIvfBuild`'s native methods) |

## Build and test

```bash
cd index-vector/native
cargo test          # the unit tests, including the JVM's random sequence and writeUTF bytes
cargo build         # target/debug/libiceberg_ivf_native.{so,dylib}
```

Gradle builds the library before the module's tests (`:iceberg-index-vector:cargoBuildNativeIvf`)
and skips it, with the native tests, when `cargo` is not on `PATH`:

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@17 ./gradlew :iceberg-index-vector:test
```

The golden test (`TestNativeIvfBuild`) builds the same entries with `IvfKernel` and with this
kernel and compares the payloads byte for byte; a payload that differs is a bug in one of the two
writers, not a tolerance to loosen.
