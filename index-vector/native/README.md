# Native IVF build kernel

The Rust side of the native IVF build kernel: it takes one unit's vectors, trains the centroids,
assigns the vectors, quantizes them (RaBitQ) and writes the payload the Java reader loads. The
crate is both the JNI library the JVM loads and a library its own tests run against.

This is the native build kernel of #3144/#3149, wired into `IvfKernel` and enabled per index
definition by `options['ivf-build-kernel']='native'` (default `java`). The Java side reaches it
through `NativeIvfBuild` in `index-vector`; the payload it writes is byte-identical to the Java
kernel's, which the golden tests compare.

A build loads every vector of the unit into this process before it trains: plan for
`4 * entries * dimension` bytes of native heap, plus one copy of the encoded list bodies and one of
the payload. That memory is outside the JVM heap, so it is not covered by `-Xmx` — size the
container above it, and remember that several units may build on one executor at once. An
allocation the machine cannot serve is reported as a build error (the Java side then falls back to
its own kernel) rather than aborting the process.

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
cargo test             # the unit tests, including the JVM's random sequence and writeUTF bytes
cargo build            # target/debug/libiceberg_ivf_native.{so,dylib}, what the tests load
cargo build --release  # the profile to measure or deploy: the debug build is not comparable
```

The jar does not carry a prebuilt library: packaging a release build under
`index-vector/src/main/resources/org/apache/iceberg/index/vector/native/<os>-<arch>/` is a
deployment step (see the user guide, §4.2.1), not something the build does for you.

Gradle builds the library before the module's tests (`:iceberg-index-vector:cargoBuildNativeIvf`)
and skips it, with the native tests, when `cargo` is not on `PATH`:

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@17 ./gradlew :iceberg-index-vector:test
```

The golden test (`TestNativeIvfBuild`) builds the same entries with `IvfKernel` and with this
kernel and compares the payloads byte for byte; a payload that differs is a bug in one of the two
writers, not a tolerance to loosen.
