/**
 * 
 */
package enginerds.hazelcast.cdc.sql;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import io.debezium.config.Configuration;

/**
 * @author SGopala
 *
 */
public class OpportunityPipelineDebeziumEngine {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// Define the configuration for the embedded and MySQL connector ...
		Configuration config = Configuration.create()
				/* begin engine properties */
				.with("connector.class", "io.debezium.connector.mysql.MySqlConnector")
				.with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
				.with("offset.storage.file.filename", "/path/to/storage/offset.dat")
				.with("offset.flush.interval.ms", 60000)
				/* begin connector properties */
				.with("name", "my-sql-connector").with("database.hostname", "host.docker.internal")
				.with("database.port", 3306).with("database.user", "root").with("database.password", "P@$$w0rd1")
				.with("database.server.id", 85744).with("database.server.name", "my-app-connector")
				.with("database.history", "io.debezium.relational.history.FileDatabaseHistory")
				.with("database.history.file.filename", "/path/to/storage/dbhistory.dat").build();

		// Create the engine with this configuration ...
		EmbeddedEngine engine = EmbeddedEngine.create().using(config).notifying(this::handleEvent).build();

		// Run the engine asynchronously ...
		Executor executor = Executors.newSingleThreadExecutor();
		executor.execute(engine);

		// At some later time ...
		engine.stop();

	}

}
