package com.alibaba.analyticdb.postgresql.client;

import java.util.Map;

public interface Attributes {
	/**
	 * Sets an attribute. In case value = null attribute is removed from the attributes map. Attribute
	 * names starting with _ indicate system attributes.
	 * @param name  attribute name
	 * @param value attribute value
	 */
	Attributes setAttribute(String name, Object value);

	/**
	 * Gets an attribute
	 * @param name attribute name
	 * @return attribute value if attribute is set, <tt>null</tt> otherwise
	 */
	byte[] getAttribute(String name);

	/**
	 * Gets all attributes
	 * @return unmodifiable map of all attributes
	 */
	Map<String, byte[]> getAttributesMap();
}
