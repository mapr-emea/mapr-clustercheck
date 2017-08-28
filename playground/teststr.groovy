#!/usr/bin/env groovy

def str = """17/08/28 07:25:15 INFO mapreduce.Job: Counters: 47
\tFile System Counters
\t\tFILE: Number of bytes read=104940
\t\tFILE: Number of bytes written=189191892
\t\tFILE: Number of read operations=0
\t\tFILE: Number of large read operations=0
\t\tFILE: Number of write operations=0
\t\tMAPRFS: Number of bytes read=1540408444
\t\tMAPRFS: Number of bytes written=1540251988
\t\tMAPRFS: Number of read operations=25045822
\t\tMAPRFS: Number of large read operations=0
\t\tMAPRFS: Number of write operations=50231964
\tJob Counters 
\t\tKilled map tasks=1
\t\tLaunched map tasks=1908
\t\tLaunched reduce tasks=6
\t\tData-local map tasks=1908
\t\tTotal time spent by all maps in occupied slots (ms)=11436387
\t\tTotal time spent by all reduces in occupied slots (ms)=3825963
\t\tTotal time spent by all map tasks (ms)=11436387
\t\tTotal time spent by all reduce tasks (ms)=1275321
\t\tTotal vcore-seconds taken by all map tasks=0
\t\tTotal vcore-seconds taken by all reduce tasks=0
\t\tTotal megabyte-seconds taken by all map tasks=11710860288
\t\tTotal megabyte-seconds taken by all reduce tasks=3917786112
\t\tDISK_MILLIS_MAPS=0
\t\tDISK_MILLIS_REDUCES=0
\tMap-Reduce Framework
\t\tMap input records=5000000
\t\tMap output records=5000000
\t\tMap output bytes=510000000
\t\tMap output materialized bytes=0
\t\tInput split bytes=202248
\t\tCombine input records=0
\t\tCombine output records=0
\t\tReduce input groups=5000000
\t\tReduce shuffle bytes=520022896
\t\tReduce input records=5000000
\t\tReduce output records=5000000
\t\tSpilled Records=10000000
\t\tShuffled Maps =11448
\t\tFailed Shuffles=0
\t\tMerged Map outputs=11454
\t\tGC time elapsed (ms)=97157
\t\tCPU time spent (ms)=1426500
\t\tPhysical memory (bytes) snapshot=1276537769984
\t\tVirtual memory (bytes) snapshot=5727508140032
\t\tTotal committed heap usage (bytes)=1495168712704
\tShuffle Errors
\t\tIO_ERROR=0
\tFile Input Format Counters 
\t\tBytes Read=500000000
\tFile Output Format Counters 
\t\tBytes Written=500000000"""

println "Input split byte: " + str.tokenize('\n').find{ it.contains("Input split byte") }.tokenize('=')[1]
println "Bytes Read: " + str.tokenize('\n').find{ it.contains("Bytes Read") }.tokenize('=')[1]
println "Bytes written: " + str.tokenize('\n').find{ it.contains("Bytes Written") }.tokenize('=')[1]
println "GC time elapsed (ms): " + str.tokenize('\n').find{ it.contains("GC time elapsed") }.tokenize('=')[1]
println "CPU time spent (ms): " + str.tokenize('\n').find{ it.contains("CPU time spent") }.tokenize('=')[1]
println "Shuffled Maps: " + str.tokenize('\n').find{ it.contains("Shuffled Maps") }.tokenize('=')[1]
println "Launched map tasks" + str.tokenize('\n').find{ it.contains("Launched map tasks") }.tokenize('=')[1]
println "Launched reduce tasks" + str.tokenize('\n').find{ it.contains("Launched reduce tasks") }.tokenize('=')[1]
println "Data-local map tasks" + str.tokenize('\n').find{ it.contains("Launched reduce tasks") }.tokenize('=')[1]
println "Reduce shuffle bytes" + str.tokenize('\n').find{ it.contains("Reduce shuffle bytes") }.tokenize('=')[1]
println "Total time spent by all maps in occupied slots ms" + str.tokenize('\n').find{ it.contains("Total time spent by all maps in occupied slots") }.tokenize('=')[1]
println "Total time spent by all reduces in occupied slots ms" + str.tokenize('\n').find{ it.contains("Total time spent by all reduces in occupied slots") }.tokenize('=')[1]
println "Total time spent by all map tasks ms" + str.tokenize('\n').find{ it.contains("Total time spent by all map tasks") }.tokenize('=')[1]
println "Total megabyte-seconds taken by all map tasks" + str.tokenize('\n').find{ it.contains("Total megabyte-seconds taken by all map tasks") }.tokenize('=')[1]
println "Total megabyte-seconds taken by all reduce tasks" + str.tokenize('\n').find{ it.contains("Total megabyte-seconds taken by all reduce tasks") }.tokenize('=')[1]

