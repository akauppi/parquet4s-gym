/*
* Main4
*
* Not connected to Parquet.
*
* Using Alpakka
* Write Parquet using 'RowParquetRecord', and Akka Streams.
*
* Note: In a larger context, managed only to write the _first_ entry of a Source. So this is MVP for debugging..
*
* Usage:
*   <<
*     [sbt] > runMain Main3
*   <<
*/
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import com.github.mjakubowski84.parquet4s.{BinaryValue, IntValue, LogicalTypes, Message, ParquetStreams, ParquetWriter, RowParquetRecord, SchemaDef, Path => ParquetPath}
import com.typesafe.scalalogging.LazyLogging
import org.apache.parquet.hadoop.ParquetFileWriter.Mode
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.schema.{MessageType, Types}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
import org.apache.parquet.schema.Type.Repetition.REQUIRED

object Main3 extends LazyLogging {

  def main(args: Array[String]): Unit = {
    implicit val as: ActorSystem = ActorSystem("main")

    val pp: ParquetPath = ParquetPath("./out.3.parquet")

    val src: Source[RowParquetRecord,_] = Source(data)

    val sink = ParquetStreams.toParquetSingleFile.generic(schemaA)
      .options(wo)
      .write( pp )

    src
      //.wireTap(x => logger.debug(s"!!! $x"))
      .runWith(sink)
  }

  val data = Seq(
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

  val wo = ParquetWriter.Options(
    writeMode = Mode.OVERWRITE,
    compressionCodecName = CompressionCodecName.SNAPPY
  )
}
