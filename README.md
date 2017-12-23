# Performance comparison between Java 8u152 and 9.0.1

Summary: Noticeable performance degradation in Java 9

## Testing parameters

* Benchmark: [CompressionBenchmark.java](src/test/java/io/airlift/compress/benchmark/CompressionBenchmark.java)
* Algorithm: lz4
* Data sets: silesia corpus
* JVMs
  * 1.8.0_152, VM 25.152-b16
  * 9.0.1, VM 9.0.1+11
* Platform:
  * Linux x86_64
  * Intel Xeon CPU E5-2660 @ 2.20GHz

To reproduce:

```
mvn test-compile exec:exec \
    -Dexec.classpathScope=test \
    -Dexec.executable="java" \
    -Dexec.args="-XX:+UseG1GC -cp %classpath io.airlift.compress.benchmark.CompressionBenchmark -p algorithm=airlift_lz4 -i 20 -wi 20 -f 10"
```

Other available algorithms: airlift_lz4, airlift_lzo, airlift_snappy, airlift_zstd (decompressor only)

## Results

### Compression

![Compression](compress.png)

#### Java 8
```
Data                Throughput
silesia/dickens     185.7MB/s ±  1000.3kB/s ( 0.53%) (N = 200, α = 99.9%)
silesia/mozilla     298.1MB/s ±  1316.7kB/s ( 0.43%) (N = 200, α = 99.9%)
silesia/mr          280.3MB/s ±  1657.4kB/s ( 0.58%) (N = 200, α = 99.9%)
silesia/nci         563.4MB/s ±  3449.1kB/s ( 0.60%) (N = 200, α = 99.9%)
silesia/ooffice     227.4MB/s ±  1411.2kB/s ( 0.61%) (N = 200, α = 99.9%)
silesia/osdb        244.6MB/s ±  2113.7kB/s ( 0.84%) (N = 200, α = 99.9%)
silesia/reymont     211.3MB/s ±  1024.5kB/s ( 0.47%) (N = 200, α = 99.9%)
silesia/samba       342.9MB/s ±  1300.5kB/s ( 0.37%) (N = 200, α = 99.9%)
silesia/sao         229.6MB/s ±  1370.7kB/s ( 0.58%) (N = 200, α = 99.9%)
silesia/webster     211.7MB/s ±  1220.6kB/s ( 0.56%) (N = 200, α = 99.9%)
silesia/x-ray       600.2MB/s ±  2755.5kB/s ( 0.45%) (N = 200, α = 99.9%)
silesia/xml         434.7MB/s ±  3283.0kB/s ( 0.74%) (N = 200, α = 99.9%)
```

#### Java 9
```
Data                Throughput
silesia/dickens     181.7MB/s ±  1135.7kB/s ( 0.61%) (N = 200, α = 99.9%)
silesia/mozilla     283.0MB/s ±  1540.3kB/s ( 0.53%) (N = 200, α = 99.9%)
silesia/mr          273.1MB/s ±  1297.3kB/s ( 0.46%) (N = 200, α = 99.9%)
silesia/nci         575.1MB/s ±  2429.0kB/s ( 0.41%) (N = 200, α = 99.9%)
silesia/ooffice     211.7MB/s ±  2070.6kB/s ( 0.96%) (N = 200, α = 99.9%)
silesia/osdb        245.1MB/s ±  1867.2kB/s ( 0.74%) (N = 200, α = 99.9%)
silesia/reymont     215.5MB/s ±   593.6kB/s ( 0.27%) (N = 200, α = 99.9%)
silesia/samba       343.7MB/s ±  1982.8kB/s ( 0.56%) (N = 200, α = 99.9%)
silesia/sao         224.1MB/s ±   892.9kB/s ( 0.39%) (N = 200, α = 99.9%)
silesia/webster     212.7MB/s ±  1049.4kB/s ( 0.48%) (N = 200, α = 99.9%)
silesia/x-ray       554.8MB/s ±  2945.1kB/s ( 0.52%) (N = 200, α = 99.9%)
silesia/xml         425.5MB/s ±  1564.5kB/s ( 0.36%) (N = 200, α = 99.9%)
```

### Decompression

![Decompression](decompress.png)

#### Java 8
```
Data                Throughput
silesia/dickens     1137.2MB/s ±  7067.5kB/s ( 0.61%) (N = 200, α = 99.9%)
silesia/mozilla     1399.0MB/s ±  7903.0kB/s ( 0.55%) (N = 200, α = 99.9%)
silesia/mr          1478.1MB/s ±  8334.8kB/s ( 0.55%) (N = 200, α = 99.9%)
silesia/nci         2038.9MB/s ±    11.7MB/s ( 0.57%) (N = 200, α = 99.9%)
silesia/ooffice     1329.6MB/s ± 10065.0kB/s ( 0.74%) (N = 200, α = 99.9%)
silesia/osdb        1373.8MB/s ±  4019.3kB/s ( 0.29%) (N = 200, α = 99.9%)
silesia/reymont     1130.1MB/s ±  4929.3kB/s ( 0.43%) (N = 200, α = 99.9%)
silesia/samba       1537.5MB/s ±  8928.8kB/s ( 0.57%) (N = 200, α = 99.9%)
silesia/sao         1857.7MB/s ±    11.5MB/s ( 0.62%) (N = 200, α = 99.9%)
silesia/webster     1072.7MB/s ±  7230.7kB/s ( 0.66%) (N = 200, α = 99.9%)
silesia/x-ray       3822.6MB/s ±    36.5MB/s ( 0.95%) (N = 200, α = 99.9%)
silesia/xml         1639.4MB/s ±  6431.7kB/s ( 0.38%) (N = 200, α = 99.9%)
```

#### Java 9
```
Data                Throughput
silesia/dickens     1058.7MB/s ±  7696.4kB/s ( 0.71%) (N = 200, α = 99.9%)
silesia/mozilla     1315.6MB/s ±  7985.2kB/s ( 0.59%) (N = 200, α = 99.9%)
silesia/mr          1360.7MB/s ±  5771.3kB/s ( 0.41%) (N = 200, α = 99.9%)
silesia/nci         1930.7MB/s ±  9354.9kB/s ( 0.47%) (N = 200, α = 99.9%)
silesia/ooffice     1270.9MB/s ±  4200.7kB/s ( 0.32%) (N = 200, α = 99.9%)
silesia/osdb        1375.0MB/s ±  6446.9kB/s ( 0.46%) (N = 200, α = 99.9%)
silesia/reymont     1039.4MB/s ±  4063.4kB/s ( 0.38%) (N = 200, α = 99.9%)
silesia/samba       1425.0MB/s ±  5837.0kB/s ( 0.40%) (N = 200, α = 99.9%)
silesia/sao         1739.4MB/s ±    12.2MB/s ( 0.70%) (N = 200, α = 99.9%)
silesia/webster     1098.9MB/s ±  4343.0kB/s ( 0.39%) (N = 200, α = 99.9%)
silesia/x-ray       3416.2MB/s ±    26.8MB/s ( 0.79%) (N = 200, α = 99.9%)
silesia/xml         1602.2MB/s ±    10.4MB/s ( 0.65%) (N = 200, α = 99.9%)
```

Full benchmark output:

* [Java 8](bench-8.txt)
* [Java 9](bench-9.txt)


# perfasm output

```
mvn exec:exec \
   -Dexec.classpathScope=test \
   -Dexec.executable="java" \
   -Dexec.args="-XX:+UseG1GC -cp %classpath org.openjdk.jmh.Main io.airlift.compress.benchmark.CompressionBenchmark.decompress -p algorithm=airlift_lz4 -p name=silesia/mr -prof perfasm -f 1 -wi 10 -i 30"
```

* [Java 8](perf-8.txt)
* [Java 9](perf-9.txt)
