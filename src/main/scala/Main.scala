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
import com.github.mjakubowski84.parquet4s.ParquetSchemaResolver.TypedSchemaDef
import scala.reflect.runtime.universe.{typeOf, TypeTag}

//import com.github.mjakubowski84.parquet4s.ParquetSchemaResolver.TypedSchemaDef
import com.github.mjakubowski84.parquet4s.{Path => ParquetPath, _}
import com.typesafe.scalalogging.LazyLogging
import org.apache.parquet.hadoop.ParquetFileWriter.Mode
import org.apache.parquet.hadoop.metadata.CompressionCodecName

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
      /***
      // Write with different type each cycle
      //
      val futs: Seq[Future[_]] = Seq( (aa, pp("a")), (bb, pp("b")) ).map{
        case (data, pPath) =>
          write /*[ParquetWritable[_]]*/ (data, pPath)
            // Compilation error:
            //  <<
            //    Cannot write data of type Product with ParquetWritable[_ >: B with A <: Product
            //      with java.io.Serializable] with java.io.Serializable. Please check if there is implicit TypedSchemaDef available for each field and subfield of Product with ParquetWritable[_ >: B with A <: Product with java.io.Serializable] with java.io.Serializable.
            //  <<
      }

      Await.ready( Future.sequence(futs), Duration.Inf )  ***/

    } else if (false) {    // work-around "C"

      /*
      * Fails with compile error:
      *   <<
      *     type mismatch;
      *      found   : akka.stream.scaladsl.Sink[(some other)<empty>._1,scala.concurrent.Future[akka.Done]]
      *      required: akka.stream.Graph[akka.stream.SinkShape[Product with ParquetWritable[_ >: B with A <: Product with java.io.Serializable] with java.io.Serializable],?]
      *                .write(pPath)
      *   <<
      */
      ??? /***
      val futs: Seq[Future[_]] = Seq( (aa, pp("a")), (bb, pp("b")) ).map{
        case (data: Seq[ParquetWritable[_]], pPath) =>
          val builder = data.head.builder

          Source(data)
            .runWith(
              builder
                .options(wo)
                .write(pPath)
                  //
                  // <<
                  // <<
            )
            .map{ x =>
              logger.debug("Done.");
              x
            }
        ***/

    } else if (false) {    // work-around "D"

      ??? /***
      val futs: Seq[Future[_]] = Seq( (aa, pp("a")), (bb, pp("b")) ).map {
        case (data: Seq[ParquetWritable[_]], pPath) => write2(data, pPath)
          //
          // <<
          //    Cannot write data of type Product with ParquetWritable[_ >: B with A <: Product with java.io.Serializable] with java.io.Serializable. Please check if there is implicit TypedSchemaDef available for each field and subfield of Product with ParquetWritable[_ >: B with A <: Product with java.io.Serializable] with java.io.Serializable.
          // <<
      }***/

    } else if (true) {    // one more work-around

      // Completely acceptable rework - my application doesn't require the map.
      //
      val futs: Seq[Future[_]] = Seq(
        write( aa, pp("a") ),
        write( bb, pp("b") )
      )

      Await.ready( Future.sequence(futs), Duration.Inf )  //***/
    }
  }

  def pp(id: String): ParquetPath = ParquetPath(s"./demo.${id}.parquet")

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
  }

    def write2[T : ParquetSchemaResolver : ParquetRecordEncoder](ts: Seq[T], pPath: ParquetPath)(implicit as: ActorSystem): Future[_] = {
      import as.dispatcher

      val builder: SingleFileParquetSink.Builder[T] = ???   // implicitly

      logger.debug(s"Writing... ${pPath.name}")

      Source(ts)
        .runWith(
          //ParquetStreams.toParquetSingleFile.of[T]
          builder
            .options(wo)
            .write(pPath)
        )
        .map{ x =>
          logger.debug("Done.");
          x
        }
    }

  // !!! CHAMPION??? 🎖
  //
  /***
  def write3[T : TypeTag](ts: Seq[T], pPath: ParquetPath)(implicit as: ActorSystem): Future[_] = {
    typeOf[T] match {
      case t if t =:= typeOf[A] =>
        write(ts : Seq[A], pPath)

      case t if t =:= typeOf[B] =>
        write(ts, pPath)

      case _ => ???
        // ... I can make 50+ of these.
    }
  }
  ***/

  val wo = ParquetWriter.Options(
    writeMode = Mode.OVERWRITE,
    compressionCodecName = CompressionCodecName.SNAPPY
  )
}
