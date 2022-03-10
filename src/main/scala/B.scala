/*
* B
*
* Case class to be written to Parquet
*/
import com.github.mjakubowski84.parquet4s.{ParquetStreams, SingleFileParquetSink}

import java.time.LocalDateTime
import scala.util.Random

case class B(a: String, b: LocalDateTime) extends ParquetWritable[B]

object B {
  private
  val rAlpha = Random.alphanumeric

  def gen(): B = {
    B( rAlpha.take(4) .mkString, LocalDateTime.now() )
  }

  implicit val builder: SingleFileParquetSink.Builder[B] = ParquetStreams.toParquetSingleFile.of[B]
}