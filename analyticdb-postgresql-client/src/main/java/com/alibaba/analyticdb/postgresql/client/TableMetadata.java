package com.alibaba.analyticdb.postgresql.client;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Table Structure
 */
public class TableMetadata implements Serializable {
	/** Build a table from this */
	public static class Builder {
		TableMetadata tableMetadata;

		AtomicLong tableIdSerial = new AtomicLong(0L);

		public Builder(String tableId) {
			tableMetadata = new TableMetadata(tableId);
		}

		List<Column> columns = new ArrayList<>();

		public Builder setColumns(List<Column> columns) {
			this.columns = columns;
			return this;
		}

		public Builder addColumn(Column column) {
			columns.add(column);
			return this;
		}

		public Builder addColumns(List<Column> columns) {
			this.columns.addAll(columns);
			return this;
		}

		public Builder setTableName(TableName tableName) {
			tableMetadata.tableName = tableName;
			return this;
		}

		public void setComment(String comment) {
			tableMetadata.comment = comment;
		}

		public Builder setDistributionKeys(String[] distributionKeys) {
			tableMetadata.distributionKeys = distributionKeys;
			return this;
		}

		public void setSegmentKey(String[] segmentKey) {
			tableMetadata.segmentKey = segmentKey;
		}

		public void setNotExist(Boolean notExist) {
			tableMetadata.isNotExist = notExist;
		}

		public void setSensitive(Boolean sensitive) {
			tableMetadata.isSensitive = sensitive;
		}

		public Builder setPartitionColumnName(String partitionColumnName) {
			tableMetadata.partitionInfo = partitionColumnName;
			tableMetadata.partitionIndex = -2;
			return this;
		}

		public Builder setPartitionColumnIndex(int partitionColumnIndex) {
			tableMetadata.partitionInfo = null;
			tableMetadata.partitionIndex = partitionColumnIndex;
			return this;
		}

		public TableMetadata build() {
			tableMetadata.columns = columns.toArray(new Column[]{});
			return tableMetadata;
		}
	}

	String tableId;
	String schemaVersion;
	TableName tableName;
	Column[] columns;

	//--------table_property---------------
	String[] distributionKeys;

	/**
	 * 分段键.
	 */
	private String[] segmentKey;
	String partitionInfo;

	/**
	 * ORIENTATION "column/row".
	 */
	private String orientation = "column";

	private String comment;

	//--------------misc---------------
	private Boolean isNotExist = true;

	private Boolean isSensitive = false;

	//--------caculated_property-------
	Map<String, Integer> columnNameToIndexMapping;			/** 大小写敏感的类名到列顺序映射 */
	Map<String, Integer> lowerColumnNameToIndexMapping;		/** 全部toLowerCase的列名到列顺序映射 */
	int partitionIndex = -2;
	String[] primaryKeys;
	int[] keyIndex;
	int[] distributionKeyIndex;
	Set<String> primaryKeySet;

	//---------Deprecated lazy load------------------
	String[] columnNames = null;
	int[] columnTypes = null;
	String[] typeNames = null;

	private TableMetadata(String tableId) {
		this.tableId = tableId;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		TableMetadata that = (TableMetadata) o;
		return tableId.equals(that.tableId)
				&& schemaVersion.equals(that.schemaVersion);
	}

	@Override
	public int hashCode() {
		return Objects.hash(tableId, schemaVersion);
	}

	public void calculateProperties() {
		if (columnNameToIndexMapping == null) {
			columnNameToIndexMapping = new HashMap<>();
			List<String> primaryKeyNames = new ArrayList<>();
			for (int i = 0; i < columns.length; ++i) {
				Column column = columns[i];
				columnNameToIndexMapping.put(column.getName(), i);
				if (column.getIsPrimaryKey()) {
					primaryKeyNames.add(column.getName());
				}
			}
			// The JDBC spec says when you have duplicate columns names,
			// the first one should be returned. So load the map in
			// reverse order so the first ones will overwrite later ones.
			lowerColumnNameToIndexMapping = new HashMap<>();
			for (int i = columns.length - 1; i > -1; --i) {
				Column column = columns[i];
				lowerColumnNameToIndexMapping.put(column.getName().toLowerCase(Locale.US), i);
			}

			if (partitionIndex > -2) {
				if (partitionIndex == -1) {
					partitionInfo = null;
				} else {
					partitionInfo = columns[partitionIndex].getName();
				}
			} else {
				if (null != partitionInfo && partitionInfo.length() > 0) {
					partitionIndex = columnNameToIndexMapping.get(partitionInfo);
				} else {
					partitionIndex = -1;
				}
			}

			primaryKeys = primaryKeyNames.toArray(new String[]{});
			keyIndex = new int[primaryKeys.length];
			if (primaryKeys.length > 0) {
				primaryKeySet = new HashSet<>();
			}
			for (int i = 0; i < primaryKeys.length; ++i) {
				keyIndex[i] = columnNameToIndexMapping.get(primaryKeys[i]);
				if (primaryKeySet != null) {
					primaryKeySet.add(primaryKeys[i]);
				}
			}

			if (distributionKeys == null) {
				distributionKeys = new String[]{};
			}
			distributionKeyIndex = new int[distributionKeys.length];
			for (int i = 0; i < distributionKeyIndex.length; ++i) {
				distributionKeyIndex[i] = columnNameToIndexMapping.get(distributionKeys[i]);
			}

			String[] typeNamesTemp = new String[columns.length];
			for (int i = 0; i < columns.length; ++i) {
				typeNamesTemp[i] = columns[i].getTypeName();
			}
			this.typeNames = typeNamesTemp;

			int[] columnTypesTemp = new int[columns.length];
			for (int i = 0; i < columns.length; ++i) {
				columnTypesTemp[i] = columns[i].getType();
			}
			this.columnTypes = columnTypesTemp;

			String[] columnNamesTemp = new String[columns.length];
			for (int i = 0; i < columns.length; ++i) {
				columnNamesTemp[i] = columns[i].getName();
			}
			this.columnNames = columnNamesTemp;

		}
	}

	public String getTableId() {
		return tableId;
	}

	public String getSchemaVersion() {
		return schemaVersion;
	}

	public int getPartitionIndex() {
		return partitionIndex;
	}

	public boolean isPartitionParentTable() {
		return partitionIndex > -1;
	}

	public TableName getTableNameObj() {
		return tableName;
	}

	public String getSchemaName() {
		return tableName.getSchemaName();
	}

	public String getTableName() {
		return tableName.getTableName();
	}

	public Column[] getColumnSchema() {
		return columns;
	}

	public Column getColumn(int index) {
		return columns[index];
	}

	public Integer getColumnIndex(String columnName) {
		return columnNameToIndexMapping.get(columnName);
	}

	public Integer getLowerColumnIndex(String lowerColumnName) {
		return lowerColumnNameToIndexMapping.get(lowerColumnName);
	}

	public String[] getPrimaryKeys() {
		return primaryKeys;
	}

	public int[] getKeyIndex() {
		return keyIndex;
	}

	public String[] getDistributionKeys() {
		return distributionKeys;
	}

	public int[] getDistributionKeyIndex() {
		return distributionKeyIndex;
	}

	public String[] getSegmentKey() {
		return segmentKey;
	}

	public String getPartitionInfo() {
		return partitionInfo;
	}

	public String getOrientation() {
		return orientation;
	}

	public Boolean getNotExist() {
		return isNotExist;
	}

	public Boolean getSensitive() {
		return isSensitive;
	}

	public String getComment() {
		return comment;
	}

	public boolean isPrimaryKey(String columnName) {
		if (primaryKeySet != null) {
			return primaryKeySet.contains(columnName);
		}
		return false;
	}

	@Override
	public String toString() {
		return "TableSchema{" + "\ntableId='" + tableId + '\'' + ", \nschemaVersion='" + schemaVersion + '\'' + ", \ntableName=" + tableName + ", \ndistributionKeys=" + Arrays.toString(distributionKeys) + ", \nsegmentKey=" + Arrays.toString(segmentKey) + ", \npartitionInfo='" + partitionInfo + '\'' + ", \norientation='" + orientation + '\'' + ", \ncolumns=" + Arrays.toString(columns) + '}';
	}
}
