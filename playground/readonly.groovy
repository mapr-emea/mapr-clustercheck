def readResult = """8192+0 records in
8192+0 records out
8589934592 bytes (8.6 GB, 8.0 GiB) copied, 17.5546 s, 489 MB/s
8192+0 records in
8192+0 records out
8589934592 bytes (8.6 GB, 8.0 GiB) copied, 18.2299 s, 471 MB/s
8192+0 records in
8192+0 records out
8589934592 bytes (8.6 GB, 8.0 GiB) copied, 19.4003 s, 443 MB/s
/dev/mapper/360060e80221336005041133600000169
8589934592 bytes (8.6 GB, 8.0 GiB) copied, 19.4003 s, 443 MB/s
/dev/mapper/360060e8022133600504113360000016a
8589934592 bytes (8.6 GB, 8.0 GiB) copied, 17.5546 s, 489 MB/s
/dev/mapper/360060e8022133600504113360000016b
8589934592 bytes (8.6 GB, 8.0 GiB) copied, 18.2299 s, 471 MB/s"""
def disks = ["/dev/mapper/360060e80221336005041133600000169", "/dev/mapper/360060e8022133600504113360000016a", "/dev/mapper/360060e8022133600504113360000016b"]
def lines = readResult.tokenize('\n')
def diskTests = disks.collect { disk ->
    def dataIdx = lines.findIndexOf { it.trim() == disk.trim() }
    if (dataIdx == -1) {
        return [:]
    }
    def res = [:]
    res['disk'] = disk
    def data = lines[dataIdx + 1].replaceAll("\\(.*?\\)", "").tokenize(',')
    res['readBytes'] = data[0].trim().tokenize(' ')[0] as Long
    res['timeInSeconds'] = data[1].trim().tokenize(' ')[0] as Double
    res['throughputInMBperSecond'] = data[2].trim().tokenize(' ')[0] as Double
    return res
}

println(diskTests)