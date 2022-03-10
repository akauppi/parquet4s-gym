import com.github.mjakubowski84.parquet4s.ParquetSchemaResolver.TypedSchemaDef
import com.github.mjakubowski84.parquet4s.{ParquetStreams, SingleFileParquetSink}

import scala.util.Random

/*
* A
*
* Case class to be written to Parquet
*/
case class A(a: String, b: Int) extends ParquetWritable[A]

object A {
  private
  val vv = Vector("some","more","words")

  def gen(): A = {
    val r = new Random()

    A( vv(r.nextInt(vv.size)), r.nextInt(2000000) )
  }

  // work-around?
  //val builder: SingleFileParquetSink.Builder[A] = ParquetStreams.toParquetSingleFile.of[A]
}