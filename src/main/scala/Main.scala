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
import akka.stream.scaladsl.{Sink, Source}
import org.apache.parquet.schema.LogicalTypeAnnotation
import com.github.mjakubowski84.parquet4s.ParquetSchemaResolver.TypedSchemaDef
//import com.github.mjakubowski84.parquet4s.ParquetSchemaResolver._
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

  // Write out parquet of A, B
  //
  def main(arr: Array[String]): Unit = {
    implicit val as: ActorSystem = ActorSystem()
    import as.dispatcher    // ExecutionContext

    val aa: Seq[A] = (1 to 10).map( _ => A.gen() )
    val bb: Seq[B] = (1 to 10).map( _ => B.gen() )

    val x: TypedSchemaDef[A] = implicitly
    val xx: ValueEncoder[A] = implicitly
    val xxx: ParquetSchemaResolver[A] = implicitly

    val y: TypedSchemaDef[B] = implicitly
    val yy: ParquetRecordEncoder[B] = implicitly
    val yyy: ParquetSchemaResolver[B] = implicitly

    if (false) {    // works
      Source(aa)
        .runWith(
          ParquetStreams.toParquetSingleFile.of[A]
            .options(wo)
            .write(pp("a"))
        )

      Source(bb)
        .runWith(
          ParquetStreams.toParquetSingleFile.of[B]
            .options(wo)
            .write(pp("b"))
        )

    } else if (false) {
      ??? /***
      // Write with different type each cycle
      //
      val futs: Seq[Future[_]] = Seq( (aa, pp("a")), (bb, pp("b")) ).map{
        case (data: Seq[ParquetWritable[_]], pPath) => write(data, pPath)
            // Compilation error:
            //  <<
            //    Cannot write data of type Product with ParquetWritable[_ >: B with A <: Product
            //      with java.io.Serializable] with java.io.Serializable. Please check if there is implicit TypedSchemaDef available for each field and subfield of Product with ParquetWritable[_ >: B with A <: Product with java.io.Serializable] with java.io.Serializable.
            //  <<
      }

      Await.ready( Future.sequence(futs), Duration.Inf )  ***/

    } else if (true) {   // try #3

      val futs: Seq[Future[_]] = Seq( (aa, pp("a")), (bb, pp("b")) ).map( ??? /*write3*/ )

      Await.ready( Future.sequence(futs), Duration.Inf )

    } else {    // work-around??????
      ??? /***
      def f[T : ParquetSchemaResolver : ParquetRecordEncoder]( data: Seq[T], pPath: ParquetPath ): Sink[T,_] => {
        data.head.builder
        Source(data).runWith(data.head.builder.options(wo) .write(pPath) )
      }

      val futs: Seq[Future[_]] = Seq( (aa, pp("a")), (bb, pp("b")) ).map(f)
      ***/
    }
  }

  def pp(id: String): ParquetPath = ParquetPath(s"./demo.${id}.parquet")

  /*** disabled
  // Write with template argument
  //
  def write[T : ParquetSchemaResolver : ParquetRecordEncoder](ts: Seq[T], pPath: ParquetPath)(implicit as: ActorSystem): Future[_] = {
    import as.dispatcher

    logger.debug(s"Writing... ${pPath.name}")

    Source(ts)
      .runWith(
        ParquetStreams.toParquetSingleFile.of[T]
          .options(wo)
          .write(pPath)
      )
      .map{ x =>
        logger.debug("Done.");
        x
      }
  } ***/

  /*** disabled
  def write2[T <: ParquetWritable[T]](ts: Seq[T], pPath: ParquetPath)(implicit as: ActorSystem): Future[_] = {
    import as.dispatcher

    logger.debug(s"Writing... ${pPath.name}")

    Source(ts)
      .runWith(
        ParquetStreams.toParquetSingleFile.of[T]
          .options(wo)
          .write(pPath)
      )
      .map{ x =>
        logger.debug("Done.");
        x
      }
  } ***/

  /***
  def write3[T <: ParquetWritable[T]]( tt: Tuple2[Seq[T],ParquetPath] )(implicit as: ActorSystem): Future[_] = {
    import as.dispatcher
    val (ts,pPath) = tt

    logger.debug(s"Writing... ${pPath.name}")

    Source(ts)
      .runWith(
        ParquetStreams.toParquetSingleFile.of[T]
          .options(wo)
          .write(pPath)
      )
      .map{ x =>
        logger.debug("Done.");
        x
      }
  }***/

  val wo = ParquetWriter.Options(
    writeMode = Mode.OVERWRITE,
    compressionCodecName = CompressionCodecName.SNAPPY
  )

  /*** disabled - automatic Enumeratum encoding
  // An enum. To be written as a string.
  //
  sealed trait AB extends EnumEntry
  object AB extends Enum[AB] {
    case object A extends AB
    case object B extends AB
      //
    override val values = findValues
  }

  // Note: Sample shows 'OptionalValueEncoder' even when it's an enum-like type (with no case of missing a value). [1]
  //
  //    [1]: https://github.com/mjakubowski84/parquet4s/blob/master/examples/src/main/scala/com/github/mjakubowski84/parquet4s/CustomType.scala
  //
  implicit def enc[T <: EnumEntry]: ValueEncoder[T] =
    (data: T, _: ValueCodecConfiguration) => BinaryValue( data.entryName )

  implicit def schema[T <: EnumEntry]: TypedSchemaDef[T] =
    SchemaDef.primitive(
      PrimitiveType.PrimitiveTypeName.BINARY,
      Some( LogicalTypeAnnotation.enumType() ),
      required = true
    )
    .typed
  ***/
}
