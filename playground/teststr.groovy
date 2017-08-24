#!/usr/bin/env groovy

def str = """		CPU time spent (ms)=87050
		Physical memory (bytes) snapshot=4768301056
		Virtual memory (bytes) snapshot=31855652864
		Total committed heap usage (bytes)=6583484416
	Shuffle Errors
		IO_ERROR=0
	File Input Format Counters 
		Bytes Read=1008
	File Output Format Counters 
		Bytes Written=81
17/08/24 10:56:13 INFO fs.TestDFSIO: ----- TestDFSIO ----- : write
17/08/24 10:56:13 INFO fs.TestDFSIO:            Date & time: Thu Aug 24 10:56:13 EDT 2017
17/08/24 10:56:13 INFO fs.TestDFSIO:        Number of files: 9
17/08/24 10:56:13 INFO fs.TestDFSIO: Total MBytes processed: 73764.0
17/08/24 10:56:13 INFO fs.TestDFSIO:      Throughput mb/sec: 284.3025407005427
17/08/24 10:56:13 INFO fs.TestDFSIO: Average IO rate mb/sec: 289.5322265625
17/08/24 10:56:13 INFO fs.TestDFSIO:  IO rate std deviation: 39.36711151787961
17/08/24 10:56:13 INFO fs.TestDFSIO:     Test exec time sec: 51.381
17/08/24 10:56:13 INFO fs.TestDFSIO: """

def tokens = str.tokenize('\n')
println getDoubleValueFromTokens(tokens, "Total MBytes processed")

def getDoubleValueFromTokens(tokens, description) {
    def line = tokens.find{ it.contains(description) }
    return Double.valueOf(line.tokenize(" ")[-1])
}

