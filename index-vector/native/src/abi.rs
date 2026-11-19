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

//! The wire format between the Java bridge and this library.
//!
//! Both sides use the layout `DataOutputStream` writes: big-endian numbers and Java's
//! "modified UTF-8" strings, which carry a `u16` byte length the way `writeUTF` does and encode
//! `NUL` as two bytes. Reproducing `writeUTF` matters beyond the build request: data file paths
//! are part of the payload, and the payload has to match the JVM writer byte for byte.

/// A cursor over a frame the Java side wrote.
pub struct Reader<'a> {
    data: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, position: 0 }
    }

    fn take(&mut self, count: usize, what: &str) -> Result<&'a [u8], String> {
        let end = self
            .position
            .checked_add(count)
            .ok_or_else(|| format!("The frame overflows while reading {}", what))?;
        if end > self.data.len() {
            return Err(format!(
                "The frame ends before its {}: {} bytes needed at byte {}, {} present",
                what,
                count,
                self.position,
                self.data.len()
            ));
        }

        let slice = &self.data[self.position..end];
        self.position = end;
        Ok(slice)
    }

    pub fn i32(&mut self, what: &str) -> Result<i32, String> {
        let bytes = self.take(4, what)?;
        Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub fn i64(&mut self, what: &str) -> Result<i64, String> {
        let bytes = self.take(8, what)?;
        let mut array = [0u8; 8];
        array.copy_from_slice(bytes);
        Ok(i64::from_be_bytes(array))
    }

    pub fn f32(&mut self, what: &str) -> Result<f32, String> {
        Ok(f32::from_bits(self.i32(what)? as u32))
    }

    pub fn boolean(&mut self, what: &str) -> Result<bool, String> {
        Ok(self.take(1, what)?[0] != 0)
    }

    /// Reads a string the way `DataInputStream.readUTF` does.
    pub fn modified_utf(&mut self, what: &str) -> Result<String, String> {
        let length = self.take(2, what)?;
        let length = u16::from_be_bytes([length[0], length[1]]) as usize;
        let bytes = self.take(length, what)?;
        let mut units = Vec::with_capacity(length);
        let mut index = 0;
        while index < bytes.len() {
            let first = bytes[index];
            if first & 0x80 == 0 {
                units.push(first as u16);
                index += 1;
            } else if first & 0xE0 == 0xC0 {
                if index + 1 >= bytes.len() {
                    return Err(format!("The {} ends inside a two-byte character", what));
                }

                let unit = ((first as u16 & 0x1F) << 6) | (bytes[index + 1] as u16 & 0x3F);
                units.push(unit);
                index += 2;
            } else if first & 0xF0 == 0xE0 {
                if index + 2 >= bytes.len() {
                    return Err(format!("The {} ends inside a three-byte character", what));
                }

                let unit = ((first as u16 & 0x0F) << 12)
                    | ((bytes[index + 1] as u16 & 0x3F) << 6)
                    | (bytes[index + 2] as u16 & 0x3F);
                units.push(unit);
                index += 3;
            } else {
                return Err(format!(
                    "The {} holds a byte 0x{:02x} that is not part of a modified UTF-8 character",
                    what, first
                ));
            }
        }

        String::from_utf16(&units).map_err(|_| {
            format!(
                "The {} holds an unpaired surrogate, which this side cannot hold",
                what
            )
        })
    }

    /// Fails when the frame has bytes this reader did not consume.
    pub fn finished(&self, what: &str) -> Result<(), String> {
        if self.position == self.data.len() {
            return Ok(());
        }

        Err(format!(
            "The {} has {} trailing bytes at byte {}",
            what,
            self.data.len() - self.position,
            self.position
        ))
    }
}

/// Writes a frame the way `DataOutputStream` does.
#[derive(Default)]
pub struct Writer {
    pub bytes: Vec<u8>,
}

impl Writer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn i32(&mut self, value: i32) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub fn i64(&mut self, value: i64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub fn f32(&mut self, value: f32) {
        self.i32(value.to_bits() as i32);
    }

    pub fn boolean(&mut self, value: bool) {
        self.bytes.push(if value { 1 } else { 0 });
    }

    pub fn raw(&mut self, bytes: &[u8]) {
        self.bytes.extend_from_slice(bytes);
    }

    /// Writes a string the way `DataOutputStream.writeUTF` does.
    pub fn modified_utf(&mut self, value: &str) -> Result<(), String> {
        let mut encoded = Vec::with_capacity(value.len() + 2);
        for unit in value.encode_utf16() {
            if (0x0001..=0x007F).contains(&unit) {
                encoded.push(unit as u8);
            } else if unit <= 0x07FF {
                // NUL and the two-byte range; NUL is written as 0xC0 0x80 rather than a zero byte
                encoded.push(0xC0 | (unit >> 6) as u8);
                encoded.push(0x80 | (unit & 0x3F) as u8);
            } else {
                encoded.push(0xE0 | (unit >> 12) as u8);
                encoded.push(0x80 | ((unit >> 6) & 0x3F) as u8);
                encoded.push(0x80 | (unit & 0x3F) as u8);
            }
        }

        if encoded.len() > u16::MAX as usize {
            return Err(format!(
                "A string of {} bytes does not fit writeUTF's two-byte length",
                encoded.len()
            ));
        }

        self.bytes
            .extend_from_slice(&(encoded.len() as u16).to_be_bytes());
        self.bytes.extend_from_slice(&encoded);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{Reader, Writer};

    /// The bytes are what `DataOutputStream.writeUTF` produced on a JDK 17 for the same string:
    /// Chinese characters, an embedded NUL, a supplementary character and Latin-1 accents.
    #[test]
    fn writes_modified_utf8_like_the_jvm() {
        let value = "数据-1.parquet\u{0}\u{1F600}/ünïcode";
        let expected = "0022e695b0e68dae2d312e70617271756574c080eda0bdedb8802fc3bc6ec3af636f6465";
        let mut writer = Writer::new();
        writer.modified_utf(value).expect("the path fits writeUTF");
        assert_eq!(expected, hex(&writer.bytes));

        let mut reader = Reader::new(&writer.bytes);
        assert_eq!(value, reader.modified_utf("path").expect("round trip"));
        reader.finished("path").expect("nothing left");
    }

    #[test]
    fn refuses_a_frame_that_ends_early() {
        let mut writer = Writer::new();
        writer.i32(7);
        let mut reader = Reader::new(&writer.bytes[..2]);
        assert!(reader.i32("count").is_err());
    }

    fn hex(bytes: &[u8]) -> String {
        let mut text = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            text.push_str(&format!("{:02x}", byte));
        }

        text
    }
}
