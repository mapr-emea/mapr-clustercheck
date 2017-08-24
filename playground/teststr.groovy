#!/usr/bin/env groovy

def str = """2017-08-24 17:47:43,7718 ERROR Client fs/client/fileclient/cc/client.cc:5809 Thread: 3492 <DoWriteRPC: write resp for file RWTestSingleTest, error 116, fid 2563.33.131342, primary fid 2562.32.131294, node 10.0.0.167:5692, off 22806528,, count 131072
2017-08-24 17:47:43,7718 ERROR Client fs/client/fileclient/cc/client.cc:5809 Thread: 3495 <DoWriteRPC: write resp for file RWTestSingleTest, error 116, fid 2563.32.131340, primary fid 2562.32.131294, node 10.0.0.167:5692, off 22740992,, count 65536
2017-08-24 17:47:43,7724 ERROR Client fs/client/fileclient/cc/client.cc:5809 Thread: 3493 <DoWriteRPC: write resp for file RWTestSingleTest, error 116, fid 2563.33.131342, primary fid 2562.32.131294, node 10.0.0.167:5692, off 22937600,, count 131072
2017-08-24 17:47:43,7726 ERROR Client fs/client/fileclient/cc/client.cc:5809 Thread: 3499 <DoWriteRPC: write resp for file RWTestSingleTest, error 116, fid 2563.33.131342, primary fid 2562.32.131294, node 10.0.0.167:5692, off 23068672,, count 131072
2017-08-24 17:47:43,7727 ERROR Client fs/client/fileclient/cc/client.cc:5809 Thread: 3498 <DoWriteRPC: write resp for file RWTestSingleTest, error 116, fid 2563.33.131342, primary fid 2562.32.131294, node 10.0.0.167:5692, off 23199744,, count 131072
2017-08-24 17:47:43,7728 ERROR Client fs/client/fileclient/cc/client.cc:5809 Thread: 3496 <DoWriteRPC: write resp for file RWTestSingleTest, error 116, fid 2563.33.131342, primary fid 2562.32.131294, node 10.0.0.167:5692, off 23330816,, count 131072
2017-08-24 17:47:43,7730 ERROR Client fs/client/fileclient/cc/client.cc:5809 Thread: 3494 <DoWriteRPC: write resp for file RWTestSingleTest, error 116, fid 2563.33.131342, primary fid 2562.32.131294, node 10.0.0.167:5692, off 23461888,, count 131072
2017-08-24 17:47:43,7730 ERROR Client fs/client/fileclient/cc/client.cc:5809 Thread: 3500 <DoWriteRPC: write resp for file RWTestSingleTest, error 116, fid 2563.33.131342, primary fid 2562.32.131294, node 10.0.0.167:5692, off 23592960,, count 131072
Write rate: 133.04026696329257 M/s
Write rate: 133.14208585542335 M/s
Write rate: 132.62535172222607 M/s"""

def lines = str.tokenize('\n').findAll{ it.startsWith("Write rate")}.collect{ Double.valueOf(it.tokenize(' ')[-2]) }
println lines.sum()

