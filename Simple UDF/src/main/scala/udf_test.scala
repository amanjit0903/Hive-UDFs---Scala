package aman.pkt

import org.apache.hadoop.hive.ql.exec.UDF

class Plus2 extends UDF{

def evaluate( x:Int ): Int = {
x+2
}
}

