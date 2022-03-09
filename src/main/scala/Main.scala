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
import com.github.mjakubowski84.parquet4s.{ParquetRecordEncoder, ParquetSchemaResolver, ParquetStreams, ParquetWriter, Path => ParquetPath}
import org.apache.parquet.hadoop.ParquetFileWriter.Mode
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.concurrent.Future

object Main {
  case class A(a: String, b: Int)

  val pp: ParquetPath = ParquetPath("./demo.parquet")

  // Write out parquet
  //
  def main(arr: Array[String]): Unit = {
    implicit val system: ActorSystem = ActorSystem()

    val aa = Seq( A("abc", 234))

    if (false) {    // works
      Source(aa)
        .runWith(
          ParquetStreams.toParquetSingleFile.of[A]
            .options(wo)
            .write(pp)
        )
    } else if (true) {    // Template argument (works)
      write[A](aa)
    }
  }

  // Write with template argument (works)
  //
  def write[T : ParquetSchemaResolver : ParquetRecordEncoder](ts: Seq[T])(implicit as: ActorSystem): Future[_] = {

    Source(ts)
      .runWith(
        ParquetStreams.toParquetSingleFile.of[T]
          .options(wo)
          .write(pp)
      )
  }

  val wo = ParquetWriter.Options(
    writeMode = Mode.OVERWRITE,
    compressionCodecName = CompressionCodecName.SNAPPY
  )
}
