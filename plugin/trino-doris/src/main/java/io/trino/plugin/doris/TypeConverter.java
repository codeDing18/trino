package io.trino.plugin.doris;

import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static java.lang.Integer.parseInt;
import static java.time.ZoneOffset.UTC;

public class TypeConverter
{
    private static final int[] NANO_FACTOR = {
            -1, // 0, no need to multiply
            100_000_000, // 1 digit after the dot
            10_000_000, // 2 digits after the dot
            1_000_000, // 3 digits after the dot
            100_000, // 4 digits after the dot
            10_000, // 5 digits after the dot
            1000, // 6 digits after the dot
            100, // 7 digits after the dot
            10, // 8 digits after the dot
            1, // 9 digits after the dot
    };

    public static Type toTrinoType(String  dorisNativeColumnType) {
        // 提取括号前的部分
        String baseType = null;
        String subString = null;
        if (dorisNativeColumnType.contains("(")) {
            baseType = dorisNativeColumnType.substring(0, dorisNativeColumnType.indexOf("("));
            subString = dorisNativeColumnType.substring(dorisNativeColumnType.indexOf("("), dorisNativeColumnType.indexOf(")") + 1);
        } else {
            baseType = dorisNativeColumnType;
        }

        switch (baseType) {
            case "boolean" : return BooleanType.BOOLEAN;
            case "tinyint" : return TinyintType.TINYINT;
            case "smallint" : return SmallintType.SMALLINT;
            case "int" : return IntegerType.INTEGER;
            case "bigint" : return BigintType.BIGINT;
            // 这个trino中好像没有largeint，doris返回的arrow是varchar,所以这里也用varchar
            case "largeint" : return VarcharType.createUnboundedVarcharType();
            case "float" : return RealType.REAL;
            case "double" : return DoubleType.DOUBLE;
            // doris 返回decimal(8, 3)
            case "decimal" :
                String[] parts = subString.replace("(", "").replace(")", "").split(",");
                int precision = Integer.parseInt(parts[0].trim());  // 8
                int scale = Integer.parseInt(parts[1].trim());     // 3
                return DecimalType.createDecimalType(precision, scale);
            case "date" : return DateType.DATE;
            // 这里先默认精确到微秒，可能获取列的信息要换下
            case "datetime" : return TimestampType.TIMESTAMP_MICROS;
            // doris返回char(2)
            case "char" :
                return CharType.createCharType(Integer.valueOf(subString.substring(1, subString.indexOf(")"))));
            // doris返回varchar(20)
            case "varchar" : return VarcharType.createVarcharType(Integer.valueOf(subString.substring(1, subString.indexOf(")"))));
            // doris中的string类型会表示为text,这里直接用trino的varchar
            case "text" : return VarcharType.createUnboundedVarcharType();
            default:
                throw new UnsupportedOperationException("Unsupported type is " + dorisNativeColumnType);
        }
    }

    public static long toTrinoTimestamp(String datetime)
    {
        Instant instant = toLocalDateTime(datetime).toInstant(UTC);
        return (instant.getEpochSecond() * MICROSECONDS_PER_SECOND) + (instant.getNano() / NANOSECONDS_PER_MICROSECOND);
    }

    public static LocalDateTime toLocalDateTime(String datetime)
    {
        int dotPosition = datetime.indexOf('.');
        if (dotPosition == -1) {
            // no sub-second element
            return LocalDateTime.from(DateTimeFormatter.ISO_LOCAL_DATE_TIME.parse(datetime));
        }
        LocalDateTime result = LocalDateTime.from(DateTimeFormatter.ISO_LOCAL_DATE_TIME.parse(datetime.substring(0, dotPosition)));
        // has sub-second element, so convert to nanosecond
        String nanosStr = datetime.substring(dotPosition + 1);
        int nanoOfSecond = parseInt(nanosStr) * NANO_FACTOR[nanosStr.length()];
        return result.withNano(nanoOfSecond);
    }
}
