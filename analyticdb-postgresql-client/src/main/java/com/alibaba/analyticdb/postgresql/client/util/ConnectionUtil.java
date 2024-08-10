package com.alibaba.analyticdb.postgresql.client.util;

import com.alibaba.analyticdb.postgresql.client.Column;
import com.alibaba.analyticdb.postgresql.client.TableMetadata;
import com.alibaba.analyticdb.postgresql.client.TableName;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class ConnectionUtil {
	public static TableMetadata getTableMetadata(Connection conn, TableName tableName) throws SQLException {
		String[] columns = null;
		int[] types = null;
		String[] typeNames = null;
		DatabaseMetaData metaData = conn.getMetaData();
		List<String> primaryKeyList = new ArrayList<>();
		try (ResultSet rs = metaData.getPrimaryKeys(null, tableName.getSchemaName(), tableName.getTableName())) {
			while (rs.next()) {
				primaryKeyList.add(rs.getString(4));
			}
		}
		List<Column> columnList = new ArrayList<>();
		String escape = metaData.getSearchStringEscape();

		try (ResultSet rs = metaData.getColumns(null, escapePattern(tableName.getSchemaName(), escape), escapePattern(tableName.getTableName(), escape), "%")) {
			while (rs.next()) {
				Column column = new Column();
				column.setName(rs.getString(4));
				column.setType(rs.getInt(5));
				column.setTypeName(rs.getString(6));
				column.setPrecision(rs.getInt(7));
				column.setScale(rs.getInt(9));
				column.setAllowNull(rs.getInt(11) == 1);
				column.setComment(rs.getString(12));
				column.setDefaultValue(rs.getObject(13));
				column.setArrayType(column.getTypeName().startsWith("_"));
				column.setIsPrimaryKey(primaryKeyList.contains(column.getName()));
				columnList.add(column);
			}
		}

		// TODO partition key

		// TODO distributed key
		String oid = "";
		String sql = "SELECT oid FROM pg_class WHERE relname = '?' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '?');";
		try (PreparedStatement pstm = conn.prepareStatement(sql)) {
			pstm.setString(1, tableName.getTableName());
			pstm.setString(2, tableName.getSchemaName());

			try (ResultSet rs = pstm.executeQuery()) {
				// should only have one result
				int count = 0;
				while (rs.next()) {
					// oid in uint32, so can't be cast to int safely
					oid = rs.getInt(1) + "";
					count++;
				}

				if (count != 1) {
					throw new SQLException("Found multiple oid from database: " + tableName.getFullName());
				}
			}
		}

		TableMetadata.Builder builder = new TableMetadata.Builder(oid);
		builder.setTableName(tableName);
		builder.setSensitive(true);
		builder.setNotExist(false);
		builder.setColumns(columnList);
		// TODO get extended metadata
		// distribute key
		// primary key
		// partition key
		// storage type
		// ...
		TableMetadata metadata = builder.build();

		return metadata;
	}

	/**
	 * The last three parameters of the getColumns method use patterns for the LIKE expression, therefore, special characters in schemaName and tableName that are passed in need to be escaped.
	 * https://www.postgresql.org/docs/current/functions-matching.html#FUNCTIONS-LIKE
	 * @param pattern
	 * @param escape
	 * @return
	 */
	private static String escapePattern(String pattern, String escape) {
		return pattern.replace(escape,  escape + escape)
				.replace("%", escape + "%")
				.replace("_", escape + "_");
	}
}
