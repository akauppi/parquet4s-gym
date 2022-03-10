/*
* ParquetWritable
*
* Base class for allowing case classes to be written by parquet4s, passed as a type parameter.
*
* Without this, Scala / parquet4s see different case classes just as 'Product' and fail to understand that they would
* each have ability to be written to a Parquet file. Deriving from 'ParquetWritable' makes it explicit.
*/
import com.github.mjakubowski84.parquet4s._

abstract class ParquetWritable[T : ParquetSchemaResolver : ParquetRecordEncoder] {
  this: T =>

  // plan C
  val builder: SingleFileParquetSink.Builder[T] = ParquetStreams.toParquetSingleFile.of[T]
}
