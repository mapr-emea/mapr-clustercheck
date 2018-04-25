#!/usr/bin/env groovy

def content = """
dfsdf
"""

def tokens = content.tokenize('\n').find {
    it =~ /^[\d ]*$/
}
println(tokens)