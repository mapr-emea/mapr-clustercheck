#!/usr/bin/env groovy

def str = """2017-08-28 09:02:44,9625 ERROR Client fs/client/fileclient/cc/client.cc:5809 Thread: 4104 <DoWriteRPC: write resp for file RWTestSingleTest, error 116, fid 2939.32.131410, primary fid 2938.32.131240, node 10.0.0.239:5692, off 72941568,, count 524288
2017-08-28 09:02:44,9627 ERROR Client fs/client/fileclient/cc/client.cc:5809 Thread: 4106 <DoWriteRPC: write resp for file RWTestSingleTest, error 116, fid 2939.32.131410, primary fid 2938.32.131240, node 10.0.0.239:5692, off 77135872,, count 524288
2017-08-28 09:02:44,9628 ERROR Client fs/client/fileclient/cc/client.cc:5809 Thread: 4110 <DoWriteRPC: write resp for file RWTestSingleTest, error 116, fid 2939.32.131410, primary fid 2938.32.131240, node 10.0.0.239:5692, off 77660160,, count 524288
2017-08-28 09:02:44,9632 ERROR Client fs/client/fileclient/cc/client.cc:5809 Thread: 4105 <DoWriteRPC: write resp for file RWTestSingleTest, error 116, fid 2939.32.131410, primary fid 2938.32.131240, node 10.0.0.239:5692, off 78184448,, count 458752
Write rate: 1012.829090909091 M/s
Write rate: 1414.745396825397 M/s
17/08/28 09:03:15 INFO Configuration.deprecation: fs.default.name is deprecated. Instead, use fs.defaultFS
17/08/28 09:03:15 INFO Configuration.deprecation: fs.default.name is deprecated. Instead, use fs.defaultFS
17/08/28 09:03:15 INFO Configuration.deprecation: fs.default.name is deprecated. Instead, use fs.defaultFS
Write rate: 696.32 M/s
Write rate: 1128.2146835443039 M/s
Write rate: 1012.829090909091 M/s
2017-Aug-28 15:03:33 - INFO >>> Running local volume benchmark... be patient this can take a while.
2017-Aug-28 15:03:33 - INFO >>>>> Run test on local volume - size: 256 MB - compression: off
"""

def writeRates = str.tokenize('\n').findAll{ it.startsWith("Write rate:")}.collect{ it.substring("Write rate:".size()).tokenize(' ')[0] }
println(writeRates)
