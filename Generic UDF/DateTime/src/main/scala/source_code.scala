package aman.pkt


import org.apache.hadoop.hive.ql.exec.UDFArgumentException
import org.apache.hadoop.hive.ql.metadata.HiveException
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.io.Text
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject
import org.apache.hadoop.hive.serde2.`lazy`.LazyDate
import org.apache.hadoop.hive.serde2.io.DateWritable
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils.ReturnObjectInspectorResolver
import org.joda.time.{DateTime,Days}
import org.joda.time.format.DateTimeFormat
import scala.annotation.switch

class IS_CHURN extends GenericUDF{

   protected var inputIO: PrimitiveObjectInspector = _
   protected var out: PrimitiveObjectInspector= _

   @throws(classOf[UDFArgumentException])
   override def initialize(args: Array[ObjectInspector]): ObjectInspector={
                assert(args.length==1)
                assert(args(0).getCategory()==Category.PRIMITIVE)
                inputIO=args(0).asInstanceOf[PrimitiveObjectInspector]
                assert(inputIO.getPrimitiveCategory() == PrimitiveCategory.DATE)
                out=PrimitiveObjectInspectorFactory.writableStringObjectInspector
                return out
                }
 
   @throws(classOf[HiveException])
   override def evaluate(args: Array[DeferredObject]): Object= {
                var value = new LazyDate(args(0).get().asInstanceOf[LazyDate])
                var value1:DateWritable = value.getWritableObject()
                var today = DateTime.now().toLocalDate
                var date1= new DateTime(value1.get())
                var DiffDate = Days.daysBetween(date1.toLocalDate(),today).getDays()
                var Chn = (DiffDate: @switch) match{
                    case x if(x<60) => "ACTIVE"
                    case x if(x>60 && x<90) => "SOFT CHURN"
                    case x if(x>=90) => "CHURN"
                    case _ => "DEFAULT"
                    }
                val ret= new Text(Chn)
                return ret
            }

   override def getDisplayString(args: Array[String]): String = "Custom Error"+args.mkString(" ")


}
