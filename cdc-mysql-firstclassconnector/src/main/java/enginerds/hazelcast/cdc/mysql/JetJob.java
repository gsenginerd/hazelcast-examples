/**
 * 
 */
package enginerds.hazelcast.cdc.mysql;

/**
 * @author SGopala
 *
 */

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.cdc.CdcSinks;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.mysql.MySqlCdcSources;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.map.IMap;

public class JetJob {

	public static void main(String[] args) {
		StreamSource<ChangeRecord> source = MySqlCdcSources.mysql("people").setDatabaseAddress("host.docker.internal")
				.setDatabasePort(3306).setDatabaseUser("root").setDatabasePassword("P@$$w0rd1")
				.setClusterName("conflicts-check-dev").setDatabaseWhitelist("cc_hazelcast")
				.setTableWhitelist("cc_hazelcast.people").build();

		Pipeline pipeline = Pipeline.create();
		pipeline.readFrom(source).withoutTimestamps().peek()
				.writeTo(CdcSinks.map("people", r -> r.key().toMap().get("id"), r -> r.value().toObject(People.class).name));

		JobConfig cfg = new JobConfig().setName("mysql-people-monitor");
		HazelcastInstance hz = Hazelcast.bootstrappedInstance();
		
		// Delete map if exists
		IMap<Integer, Object> peopleMap = hz.getMap("people");
		if (peopleMap != null)
			peopleMap.destroy();
		
		hz.getJet().newJob(pipeline, cfg);
	}

}
