/*
* Main2
*
* Write Parquet using 'RowParquetRecord'.
*
* Note: There are two ways to define schemas. The code has both (one active). They work about the same, producing
*     slightly different output metadata.
*
* Usage:
*   <<
*     [sbt] > runMain Main2
*   <<
*/
import com.github.mjakubowski84.parquet4s.{BinaryValue, IntValue, LogicalTypes, Message, ParquetWriter, RowParquetRecord, SchemaDef, Path => ParquetPath}
import org.apache.parquet.hadoop.ParquetFileWriter.Mode
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.schema.{MessageType, Types}

import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
import org.apache.parquet.schema.Type.Repetition.REQUIRED

object Main2 {

  def main(args: Array[String]): Unit = {
    println("Hello!")

    val pp: ParquetPath = ParquetPath("./out.a.parquet")

    ParquetWriter
      .generic(schemaA)
      .options(wo)
      .writeAndClose( pp, data )
  }

  val data: Iterable[RowParquetRecord] = Seq[RowParquetRecord](
    RowParquetRecord( "a" -> IntValue(1), "b" -> BinaryValue("D") ),
    RowParquetRecord( "a" -> IntValue(2), "b" -> BinaryValue("E") )
  )

  /*** Works
  val schemaA: MessageType = Message( None,
    SchemaDef.primitive( PrimitiveType.PrimitiveTypeName.INT32, Option(LogicalTypes.Int32Type), required = false )("a"),
    SchemaDef.primitive( PrimitiveType.PrimitiveTypeName.BINARY, Option(LogicalTypes.StringType), required = false )("b")
  )
  ***/

  /* Based on -> https://github.com/mjakubowski84/parquet4s/blob/master/examples/src/main/scala/com/github/mjakubowski84/parquet4s/akka/WriteAndReadGenericAkkaApp.scala#L20
  *
  * <<
  *   >>> import pyarrow.dataset as dss
  *   >>> fn = "out.a.parquet"
  *   >>> ds = dss.dataset(fn)
  *   >>> ds.schema
  *   a: int32 not null
  *   b: string not null
  *   -- schema metadata --
  *   MadeBy: 'https://github.com/mjakubowski84/parquet4s'
  *   >>>
  * <<
  */
  val schemaA: MessageType = {
    val bld = Types.buildMessage()

    Seq( (INT32, Some(LogicalTypes.Int32Type), "a"), (BINARY, Some(LogicalTypes.StringType), "b") ).foreach{
      case (a,Some(b),fieldName) =>
        bld.addField( Types.primitive(a, REQUIRED).as(b) .named(fieldName) )
    }
    bld.named("just_testing")
  }

  //class A
  //class A(a: Int, b: String)

  val wo = ParquetWriter.Options(
    writeMode = Mode.OVERWRITE,
    compressionCodecName = CompressionCodecName.SNAPPY
  )
}
