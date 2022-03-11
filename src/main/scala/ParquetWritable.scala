/*
* ParquetWritable
*
* Base class for allowing case classes to be written by parquet4s, passed as a type parameter.
*
* Without this, Scala / parquet4s see different case classes just as 'Product' and fail to understand that they would
* each have ability to be written to a Parquet file. Deriving from 'ParquetWritable' makes it explicit.
*/
import akka.stream.scaladsl.Sink
import com.github.mjakubowski84.parquet4s._
import org.apache.parquet.hadoop.ParquetFileWriter.Mode
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import com.github.mjakubowski84.parquet4s.{Path => ParquetPath, _}

abstract class ParquetWritable[T : ParquetSchemaResolver : ParquetRecordEncoder] /*(dim: String)*/ {
  this: T =>
  import ParquetWritable._

  // plan C
  val builder: SingleFileParquetSink.Builder[T] = ParquetStreams.toParquetSingleFile.of[T]

  // plan F
  /***
  private
  val pPath: ParquetPath = ParquetPath(s"./out.${dim}.parquet")

  val pqtSink: Sink[T,_] =
    ParquetStreams.toParquetSingleFile.of[T]
      .options(wo)
      .write( pPath )
  ***/
}

object ParquetWritable {

  /***private
  val wo = ParquetWriter.Options(
    writeMode = Mode.OVERWRITE,
    compressionCodecName = CompressionCodecName.SNAPPY
  )***/
}