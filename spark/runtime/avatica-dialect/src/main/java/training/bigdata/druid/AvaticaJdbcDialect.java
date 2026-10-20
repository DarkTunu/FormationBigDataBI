package training.bigdata.druid;

import java.sql.Types;

import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;

import scala.Option;

/** Maps Avatica's unbounded character columns to Spark strings. */
public final class AvaticaJdbcDialect extends JdbcDialect {
  @Override
  public boolean canHandle(String url) {
    return url != null && url.startsWith("jdbc:avatica:");
  }

  @Override
  public Option<DataType> getCatalystType(
      int sqlType, String typeName, int size, MetadataBuilder metadata) {
    if (sqlType == Types.CHAR
        || sqlType == Types.VARCHAR
        || sqlType == Types.LONGVARCHAR
        || sqlType == Types.NCHAR
        || sqlType == Types.NVARCHAR
        || sqlType == Types.LONGNVARCHAR) {
      return Option.apply(DataTypes.StringType);
    }
    return Option.empty();
  }
}
