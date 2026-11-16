/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

class Bm25RangeProbe {
  static final int DOCS = 200_000;
  static final int TERMS = 4096;
  static final int WIDTH = 16;
  static final int K = 10;
  static final int DIRECTORY_ENTRY_SIZE = Long.BYTES + Integer.BYTES;
  static final int POSTING_SIZE = Integer.BYTES + Byte.BYTES;
  static final int HEADER = TERMS * DIRECTORY_ENTRY_SIZE;
  static final String FILE = "term-postings.bin";
  static final HttpClient HTTP =
      HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();

  static void build(Path lucene, Path flat) throws Exception {
    var postings = new ArrayList<List<Integer>>();
    for (int i = 0; i < TERMS; i++) postings.add(new ArrayList<>());
    var random = new Random(42);
    try (var dir = FSDirectory.open(lucene);
        var analyzer = new WhitespaceAnalyzer();
        var writer =
            new IndexWriter(dir, new IndexWriterConfig(analyzer).setRAMBufferSizeMB(128))) {
      for (int doc = 0; doc < DOCS; doc++) {
        int[] words = new int[WIDTH];
        int rare = doc % 200 == 0 ? 1 + random.nextInt(4) : 0;
        int common = doc % 5 != 0 ? 1 + random.nextInt(4) : 0;
        Arrays.fill(words, rare, rare + common, 1);
        for (int i = rare + common; i < WIDTH; i++) words[i] = 2 + random.nextInt(TERMS - 2);
        var termFreq = new HashMap<Integer, Integer>();
        var body = new StringBuilder();
        for (int term : words) {
          termFreq.merge(term, 1, Integer::sum);
          body.append("t").append(term).append(' ');
        }
        for (var entry : termFreq.entrySet())
          postings.get(entry.getKey()).add(doc * (WIDTH + 1) + entry.getValue());
        var document = new Document();
        document.add(new TextField("body", body.toString(), Field.Store.NO));
        writer.addDocument(document);
      }
      writer.forceMerge(1);
    }

    try (var out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(flat)))) {
      long offset = HEADER;
      for (var term : postings) {
        out.writeLong(offset);
        out.writeInt(term.size());
        offset += (long) term.size() * POSTING_SIZE;
      }
      for (var term : postings) {
        for (int entry : term) {
          out.writeInt(entry / (WIDTH + 1));
          out.writeByte(entry % (WIDTH + 1));
        }
      }
    }
  }

  static class Remote {
    final URI server;
    final int blockSize;
    final Map<String, byte[]> cache = new HashMap<>();
    long bytes;
    int requests;

    Remote(URI server, int blockSize) {
      this.server = server;
      this.blockSize = blockSize;
    }

    byte[] block(String name, long offset, long length) throws IOException {
      long start = offset / blockSize * blockSize;
      String key = name + ":" + start;
      byte[] data = cache.get(key);
      if (data != null) return data;
      long end = Math.min(start + blockSize, length) - 1;
      try {
        var req =
            HttpRequest.newBuilder(server.resolve(name))
                .header("Range", "bytes=" + start + "-" + end)
                .GET()
                .build();
        var res = HTTP.send(req, HttpResponse.BodyHandlers.ofByteArray());
        data = res.body();
        if (res.statusCode() != 206 || data.length != end - start + 1)
          throw new IOException(res.toString());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
      bytes += data.length;
      requests++;
      cache.put(key, data);
      return data;
    }
  }

  static class RangeInput extends IndexInput {
    final Remote remote;
    final String name;
    final long length;
    final long base;
    final long window;
    long position;

    RangeInput(Remote remote, String name, long length, long base, long window) {
      super(name);
      this.remote = remote;
      this.name = name;
      this.length = length;
      this.base = base;
      this.window = window;
    }

    public byte readByte() throws IOException {
      if (position >= window) throw new EOFException();
      long absolute = base + position;
      byte[] data = remote.block(name, absolute, length);
      position++;
      return data[(int) (absolute % remote.blockSize)];
    }

    public void readBytes(byte[] b, int offset, int len) throws IOException {
      if (len < 0 || position + len > window) throw new EOFException();
      while (len > 0) {
        long absolute = base + position;
        byte[] data = remote.block(name, absolute, length);
        int source = (int) (absolute % remote.blockSize);
        int copy = Math.min(len, data.length - source);
        System.arraycopy(data, source, b, offset, copy);
        position += copy;
        offset += copy;
        len -= copy;
      }
    }

    public long getFilePointer() {
      return position;
    }

    public void seek(long offset) throws IOException {
      if (offset < 0 || offset > window) throw new EOFException();
      position = offset;
    }

    public IndexInput slice(String desc, long offset, long len) throws IOException {
      if (offset < 0 || len < 0 || offset + len > window) throw new EOFException();
      return new RangeInput(remote, name, length, base + offset, len);
    }

    public long length() {
      return window;
    }

    public void close() {}
  }

  static int[] lucene(Path dir, Remote remote, int term) throws IOException {
    try (var filter =
            new FilterDirectory(FSDirectory.open(dir)) {
              public IndexInput openInput(String name, IOContext context) throws IOException {
                long size = fileLength(name);
                return new RangeInput(remote, "lucene/" + name, size, 0, size);
              }

              public ChecksumIndexInput openChecksumInput(String name, IOContext context)
                  throws IOException {
                return new BufferedChecksumIndexInput(openInput(name, context));
              }
            };
        var reader = DirectoryReader.open(filter)) {
      var result = new IndexSearcher(reader).search(new TermQuery(new Term("body", "t" + term)), K);
      return Arrays.stream(result.scoreDocs).mapToInt(h -> h.doc).toArray();
    }
  }

  static int[] flat(Path path, Remote remote, int term) throws IOException {
    long size = Files.size(path);
    try (var input = new RangeInput(remote, FILE, size, 0, size)) {
      input.seek((long) term * DIRECTORY_ENTRY_SIZE);
      long offset = Long.reverseBytes(input.readLong());
      int count = Integer.reverseBytes(input.readInt());
      input.seek(offset);
      var top = new PriorityQueue<Integer>();
      for (int i = 0; i < count; i++) {
        int doc = Integer.reverseBytes(input.readInt());
        int tf = input.readByte();
        int rank = tf * DOCS + DOCS - 1 - doc;
        if (top.size() < K) top.add(rank);
        else if (rank > top.element()) {
          top.remove();
          top.add(rank);
        }
      }
      return top.stream()
          .sorted(Comparator.reverseOrder())
          .mapToInt(v -> DOCS - 1 - v % DOCS)
          .toArray();
    }
  }

  static void respond(Path folder, HttpExchange exchange) throws IOException {
    try (exchange) {
      Path file = folder.resolve(exchange.getRequestURI().getPath().substring(1)).normalize();
      if (!file.startsWith(folder) || !Files.isRegularFile(file)) {
        exchange.sendResponseHeaders(404, -1);
        return;
      }
      long size = Files.size(file);
      String[] range = exchange.getRequestHeaders().getFirst("Range").substring(6).split("-");
      long start = Long.parseLong(range[0]);
      long end = Long.parseLong(range[1]);
      int len = Math.toIntExact(end - start + 1);
      byte[] bytes = new byte[len];
      try (var in = new RandomAccessFile(file.toFile(), "r")) {
        in.seek(start);
        in.readFully(bytes);
      }
      exchange.getResponseHeaders().set("Content-Range", "bytes " + start + "-" + end + "/" + size);
      exchange.sendResponseHeaders(206, len);
      exchange.getResponseBody().write(bytes);
    }
  }

  public static void main(String[] args) throws Exception {
    Path work = Files.createTempDirectory("bm25-range-probe");
    Path dir = Files.createDirectory(work.resolve("lucene"));
    Path flat = work.resolve(FILE);
    build(dir, flat);
    try (var files = Files.list(dir)) {
      long bytes = files.mapToLong(p -> p.toFile().length()).sum();
      System.out.printf(
          "docs=%d terms=%d tokens/doc=%d k=%d lucene_bytes=%d flat_bytes=%d%n",
          DOCS, TERMS, WIDTH, K, bytes, Files.size(flat));
    }
    var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", exchange -> respond(work, exchange));
    server.start();
    try {
      URI uri = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/");
      for (int block : new int[] {4096, 65536, 262144}) {
        for (int term : new int[] {0, 1}) {
          var a = new Remote(uri, block);
          var b = new Remote(uri, block);
          int[] la = lucene(dir, a, term);
          int[] fb = flat(flat, b, term);
          if (!Arrays.equals(la, fb))
            throw new IllegalStateException(Arrays.toString(la) + " != " + Arrays.toString(fb));
          System.out.printf(
              "block=%d term=%s lucene_gets=%d lucene_read=%d flat_gets=%d flat_read=%d"
                  + " same_top_k=%s%n",
              block,
              term == 0 ? "rare" : "common",
              a.requests,
              a.bytes,
              b.requests,
              b.bytes,
              Arrays.equals(la, fb));
        }
      }
    } finally {
      server.stop(0);
      try (var files = Files.walk(work)) {
        for (Path path : files.sorted(Comparator.reverseOrder()).toList()) {
          Files.delete(path);
        }
      }
    }
  }
}
