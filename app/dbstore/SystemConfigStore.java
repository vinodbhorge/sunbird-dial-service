/**
 * 
 */
package dbstore;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import commons.AppConfig;
import utils.CassandraConnector;
import utils.DialCodeEnum;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author pradyumnaZ
 *
 */
public class SystemConfigStore extends CassandraStore {

	public SystemConfigStore() {
		super();
		String keyspace = AppConfig.config.hasPath("system.config.keyspace.name")
				? AppConfig.config.getString("system.config.keyspace.name")
				: "dialcode_store";
		String table = AppConfig.config.hasPath("system.config.table") ? AppConfig.config.getString("system.config.table")
				: "system_config";
		initialise(keyspace, table, "SystemConfig");
	}

	public Double getDialCodeIndex() throws Exception {
		List<Row> rows = read(DialCodeEnum.prop_key.name(), DialCodeEnum.dialcode_max_index.name());
		Row row = rows.get(0);
		return Double.valueOf(row.getString(DialCodeEnum.prop_value.name()));
	}

	public void setDialCodeIndex(double maxIndex) throws Exception {
		Map<String, Object> data = new HashMap<String, Object>();
		data.put(DialCodeEnum.prop_value.name(), String.valueOf((int) maxIndex));
		update(DialCodeEnum.prop_key.name(), DialCodeEnum.dialcode_max_index.name(), data, null);
	}

	/**
	 * Atomically increments the dialcode_max_index using a Cassandra
	 * Lightweight Transaction (compare-and-set). Retries on CAS conflict to
	 * safely support concurrent requests and multiple service instances when
	 * Redis is disabled.
	 *
	 * @return the new (incremented) index value
	 * @throws Exception if the row is missing or all retries are exhausted
	 */
	public Double getAndIncrementDialCodeIndex() throws Exception {
		int maxRetries = 10;
		for (int i = 0; i < maxRetries; i++) {
			List<Row> rows = read(DialCodeEnum.prop_key.name(), DialCodeEnum.dialcode_max_index.name());
			if (rows.isEmpty())
				throw new Exception("dialcode_max_index not found in system_config");
			Row row = rows.get(0);
			String currentStr = row.getString(DialCodeEnum.prop_value.name());
			double current = Double.valueOf(currentStr);
			double next = current + 1;
			String nextStr = String.valueOf((int) next);
			Statement stmt = QueryBuilder.update(getKeyspace(), getTable())
					.with(QueryBuilder.set(DialCodeEnum.prop_value.name(), nextStr))
					.where(QueryBuilder.eq(DialCodeEnum.prop_key.name(), DialCodeEnum.dialcode_max_index.name()))
					.onlyIf(QueryBuilder.eq(DialCodeEnum.prop_value.name(), currentStr));
			ResultSet rs = CassandraConnector.getSession().execute(stmt);
			Row applied = rs.one();
			if (applied.getBool("[applied]")) {
				return next;
			}
			// Another instance won the CAS race; re-read and retry.
		}
		throw new Exception("Failed to allocate DIAL code index after " + maxRetries
				+ " retries due to high concurrency.");
	}
}
