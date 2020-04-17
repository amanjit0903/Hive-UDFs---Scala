package aman.pkt

import org.apache.hadoop.hive.ql.exec.UDFArgumentException
import org.apache.hadoop.hive.ql.metadata.HiveException
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject
import org.apache.hadoop.hive.serde2.`lazy`.LazyInteger
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils.ReturnObjectInspectorResolver

class GenUDFMult2 extends GenericUDF{
   
   protected var inputIO: PrimitiveObjectInspector = _
   protected var out: PrimitiveObjectInspector= _

   @throws(classOf[UDFArgumentException])
   override def initialize(args: Array[ObjectInspector]): ObjectInspector={
                assert(args.length==1)
                assert(args(0).getCategory()==Category.PRIMITIVE)
                inputIO=args(0).asInstanceOf[PrimitiveObjectInspector]
                assert(inputIO.getPrimitiveCategory() == PrimitiveCategory.INT)
                out=PrimitiveObjectInspectorFactory.writableIntObjectInspector
                return out
                }

   @throws(classOf[HiveException])
   override def evaluate(args: Array[DeferredObject]): Object= {
                var value = new LazyInteger(args(0).get().asInstanceOf[LazyInteger])
                var value1:IntWritable = value.getWritableObject()
                value1.set(value1.get()*2)
                return value1
            }
   
   override def getDisplayString(args: Array[String]): String = "Custom Error"+args.mkString(" ")


}
