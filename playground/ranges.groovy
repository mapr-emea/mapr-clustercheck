#!/usr/bin/env groovy

def str = "host[1-12,15].local" =~ /\[(.*?)\]/
println str[0][1]


