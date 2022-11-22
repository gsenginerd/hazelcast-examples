/**
 * 
 */
package enginerds.hazelcast.cdc.sql;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.cdc.CdcSinks;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.DebeziumCdcSources;
import com.hazelcast.jet.cdc.DebeziumCdcSources.Builder;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.map.IMap;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnector;

/**
 * @author SGopala
 *
 */
public class OpportunityPipeline {
	/**
	 * @param args
	 */
	public static void main(String[] args) {

		DebeziumCdcSources.Builder<ChangeRecord> builder = DebeziumCdcSources.debezium("debezium-sqlserver-connector",
				"io.debezium.connector.sqlserver.SqlServerConnector");
		builder.setProperty("connector.class", "io.debezium.connector.sqlserver.SqlServerConnector");
		builder.setProperty("database.hostname", "WLA-GSUBT14");
		builder.setProperty("database.port", "1433");
		builder.setProperty("database.user", "sa");
		builder.setProperty("database.password", "P@ssword");
		builder.setProperty("database.dbname", "CONFLICT_CHECK");
		builder.setProperty("table.whitelist", "dbo.SF_OPPORTUNITY");
		builder.setProperty("database.server.name", "sqlserver");

		StreamSource<ChangeRecord> source = builder.build();

		Pipeline pipeline = Pipeline.create();
		pipeline.readFrom(source).withoutTimestamps().writeTo(Sinks.logger());

		JobConfig cfg = new JobConfig().setName("SQLServer-Monitor");
		HazelcastInstance hz = Hazelcast.bootstrappedInstance();
		hz.getJet().newJob(pipeline, cfg);
	}

}
