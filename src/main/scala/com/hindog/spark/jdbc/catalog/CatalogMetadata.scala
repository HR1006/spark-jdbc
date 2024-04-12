package com.hindog.spark.jdbc.catalog

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import java.sql.{Connection, ResultSet}

class CatalogMetadata(conn: Connection) extends AbstractMetadata(conn) {
  override def fetch(): List[Row] = {
    if (isDebugEnabled) {
      log.info("Fetching Catalog Metadata")
    }
    withStatement { stmt =>
      val catalogName = CatalogMetadata.catalogName
      if (catalogName.nonEmpty) {
        val rs = stmt.executeQuery(s"SELECT '$catalogName' as catalogName")
        extractResult(rs)(getRow)
      } else {
        List.empty[Row]
      }
    }
  }

  override def schema: StructType = StructType(Seq(StructField("TABLE_CAT", StringType, nullable = false)))

  override def getRow(rs: ResultSet): Row = new GenericRowWithSchema(Array[Any](rs.getString("catalogName")), schema)
}

object CatalogMetadata {
  def catalogName: String = sys.props.getOrElse("com.hindog.spark.jdbc.catalog.name", "")
}
