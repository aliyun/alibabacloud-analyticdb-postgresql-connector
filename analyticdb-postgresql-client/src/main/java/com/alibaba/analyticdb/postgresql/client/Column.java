package com.alibaba.analyticdb.postgresql.client;

import java.io.Serializable;

public class Column implements Serializable {
	private String name;
	private String typeName;
	private int type; // JDBC type
	private String comment;
	private Boolean allowNull;
	private Boolean isPrimaryKey;
	private Object defaultValue;
	private Boolean arrayType;
	private int precision;		// using for decimal
	private int scale;			// using for decimal

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getTypeName() {
		return typeName;
	}

	public void setTypeName(String typeName) {
		this.typeName = typeName;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	public Boolean getAllowNull() {
		return allowNull != null && allowNull;
	}

	public void setAllowNull(Boolean allowNull) {
		this.allowNull = allowNull;
	}

	public Boolean getIsPrimaryKey() {
		return isPrimaryKey != null && isPrimaryKey;
	}

	public void setIsPrimaryKey(Boolean primaryKey) {
		isPrimaryKey = primaryKey;
	}

	public Object getDefaultValue() {
		return defaultValue;
	}

	public void setDefaultValue(Object defaultValue) {
		this.defaultValue = defaultValue;
	}

	public Boolean getArrayType() {
		return arrayType;
	}

	public void setArrayType(Boolean arrayType) {
		this.arrayType = arrayType;
	}

	public int getPrecision() {
		return precision;
	}

	public void setPrecision(int precision) {
		this.precision = precision;
	}

	public boolean isSerial() {
		return "serial".equals(typeName) || "bigserial".equals(typeName) || "smallserial".equals(typeName);
	}

	public int getScale() {
		return scale;
	}

	public void setScale(int scale) {
		this.scale = scale;
	}

	@Override
	public String toString() {
		return "Column{" +
				"name='" + name + '\'' +
				", typeName='" + typeName + '\'' +
				", type=" + type +
				", comment='" + comment + '\'' +
				", allowNull=" + allowNull +
				", isPrimaryKey=" + isPrimaryKey +
				", defaultValue=" + defaultValue +
				", arrayType=" + arrayType +
				", precision=" + precision +
				", scale=" + scale +
				'}';
	}
}
