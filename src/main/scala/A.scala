/*
* A
*
* Case class to be written to Parquet
*/
import com.github.mjakubowski84.parquet4s.ParquetSchemaResolver.TypedSchemaDef
import com.github.mjakubowski84.parquet4s.SchemaDef

import scala.util.Random

case class A(a: String, b: Int) extends ParquetWritable[A] //("a")

object A {
  //import SchemaDef2._

  private
  val vv = Vector("some","more","words")

  def gen(): A = {
    val r = new Random()

    A( vv(r.nextInt(vv.size)), r.nextInt(2000000) )
  }

  // It might be a good idea to allow explicit schema creation - at least as an option.
  //
  /***
  implicit val schema: TypedSchemaDef = SchemaDef.group(
    SchemaDef2.str,
    SchemaDef2.u32
  )
  .typed
  ***/
}

/***
// TEMP: Could be elsewhere
object SchemaDef2 {
  import org.apache.parquet.schema.{LogicalTypeAnnotation, PrimitiveType}
  import PrimitiveType.PrimitiveTypeName.{BINARY, INT32}
  import LogicalTypeAnnotation.{intType, StringLogicalTypeAnnotation}
  import com.github.mjakubowski84.parquet4s.PrimitiveSchemaDefs._

  val str: TypedSchemaDef[String] = implicitly     // SchemaDef.primitive( BINARY, Some( StringLogicalTypeAnnotation ), required = true ) .typed
  val u32: TypedSchemaDef[Int] = SchemaDef.primitive( INT32, Some( intType(32,false) ), required = true ) .typed
}
***/