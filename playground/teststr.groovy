#!/usr/bin/env groovy

def str = """20170824054803,10.0.0.24,59524,10.0.0.167,5001,3,0.0-1.5,134217728,702695441
20170824054803,10.0.0.24,59526,10.0.0.167,5001,4,0.0-1.6,134217728,690317875
20170824054803,10.0.0.24,0,10.0.0.167,5001,-1,0.0-1.6,268435456,1380635751"""
def tok = str.tokenize(',')
def dataCopiedInBytes = tok[-2]
def throughputInBitPerSecond = tok[-1]
println dataCopiedInBytes
println throughputInBitPerSecond