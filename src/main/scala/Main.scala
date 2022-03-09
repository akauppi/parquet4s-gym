/*
* Sample for using parquet4s
*
* Testing out the encoders.
*
* Usage:
*   <<
*     [sbt] > run
*   <<
*/
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import com.github.mjakubowski84.parquet4s.ParquetSchemaResolver._
import com.github.mjakubowski84.parquet4s.{Path => ParquetPath, _}
import com.typesafe.scalalogging.LazyLogging
import enumeratum._
import org.apache.parquet.hadoop.ParquetFileWriter.Mode
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation
import org.apache.parquet.schema.PrimitiveType

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.Duration

object Main extends LazyLogging {

  case class Sample(a: String, b: Int)
  case class SampleWithEnum(a: AB, b: Int)

  // Write out parquet
  //
  def main(arr: Array[String]): Unit = {
    implicit val as: ActorSystem = ActorSystem()
    import as.dispatcher    // ExecutionContext

    val aa = Seq( Sample("abc", 234) )
    val bb = Seq( SampleWithEnum(AB.B, 345) )

    if (false) {    // works
      val pp: ParquetPath = ParquetPath("./demo.parquet")

      val x: TypedSchemaDef[AB] = implicitly    // ok

      // If there's no import of '[...].parquet4s.ParquetSchemaResolver._':
      //    "could not find implicit value for parameter e: com.github.mjakubowski84.parquet4s.ParquetSchemaResolver.TypedSchemaDef[Main.SampleWithEnum]"
      //
      // If there IS:
      //    "ambiguous implicit values:
      //      both value stringSchema in trait PrimitiveSchemaDefs of type com.github.mjakubowski84.parquet4s.ParquetSchemaResolver.TypedSchemaDef[String]
      //      and value charSchema in trait PrimitiveSchemaDefs of type com.github.mjakubowski84.parquet4s.ParquetSchemaResolver.TypedSchemaDef[Char]
      //      match expected type com.github.mjakubowski84.parquet4s.SchemaDef
      //    "
      // ??
      //
      val xx: TypedSchemaDef[SampleWithEnum] = implicitly

      Source(bb)
        .runWith(
          ParquetStreams.toParquetSingleFile.of[SampleWithEnum]
            .options(wo)
            .write(pp)
        )

    } else if (true) {    // Template argument (works)
      write[Sample](aa, 0)

    } else if (false) {

      // Write with different type each cycle
      //
      val futs: Seq[Future[_]] = Seq(aa, bb).zipWithIndex.map{
        case (ss, i) =>
          ??? // write(ss, i)    // "Cannot write data of type Product with java.io.Serializable. Please check if there is implicit TypedSchemaDef available [...]"
      }

      Await.ready( Future.sequence(futs), Duration.Inf )
    }
  }

  // Write with template argument (works)
  //
  def write[T : ParquetSchemaResolver : ParquetRecordEncoder](ts: Seq[T], index: Int)(implicit as: ActorSystem): Future[_] = {
    import as.dispatcher

    val pp: ParquetPath = ParquetPath(s"./demo.${index}.parquet")

    logger.debug(s"Writing... ${pp.name}")

    Source(ts)
      .runWith(
        ParquetStreams.toParquetSingleFile.of[T]
          .options(wo)
          .write(pp)
      )
      .map{ x =>
        logger.debug("Done.");
        x
      }
  }

  val wo = ParquetWriter.Options(
    writeMode = Mode.OVERWRITE,
    compressionCodecName = CompressionCodecName.SNAPPY
  )

  // An enum. To be written as a string.
  //
  sealed trait AB extends EnumEntry
  object AB extends Enum[AB] {
    case object A extends AB
    case object B extends AB
      //
    override val values = findValues
  }

  /***
  // tbd. Make generic to 'T <: EnumEntry'
  //
  // Note: Sample shows 'OptionalValueEncoder' even when it's an enum-like type (with no case of missing a value). [1]
  //
  //    [1]: https://github.com/mjakubowski84/parquet4s/blob/master/examples/src/main/scala/com/github/mjakubowski84/parquet4s/CustomType.scala
  //
  implicit val abEncoder: OptionalValueEncoder[AB] =
    (data: AB, _: ValueCodecConfiguration) => BinaryValue( data.entryName )

  implicit val schema: TypedSchemaDef[AB] =
    SchemaDef
      .primitive(
        primitiveType         = PrimitiveType.PrimitiveTypeName.BINARY,
        required              = true,
        logicalTypeAnnotation = Option(LogicalTypeAnnotation.stringType())
      )
      .typed
  ***/

  // tbd. Make generic to 'T <: EnumEntry'
  //
  // Note: Sample shows 'OptionalValueEncoder' even when it's an enum-like type (with no case of missing a value). [1]
  //
  //    [1]: https://github.com/mjakubowski84/parquet4s/blob/master/examples/src/main/scala/com/github/mjakubowski84/parquet4s/CustomType.scala
  //
  implicit def enc[T <: EnumEntry](v: T): OptionalValueEncoder[T] =
    (data: T, _: ValueCodecConfiguration) => BinaryValue( data.entryName )

  implicit def schema[T <: EnumEntry]: TypedSchemaDef[T] =
    SchemaDef.primitive(
      PrimitiveType.PrimitiveTypeName.BINARY,
      Some( StringLogicalTypeAnnotation ),
      required = true
    )
    .typed
}
