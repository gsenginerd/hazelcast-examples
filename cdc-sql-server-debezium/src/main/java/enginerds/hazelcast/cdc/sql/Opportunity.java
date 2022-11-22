/**
 * 
 */
package enginerds.hazelcast.cdc.sql;

import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author SGopala
 *
 */
public class Opportunity implements Serializable {

	@JsonProperty("id")
	public int id;

	@JsonProperty("name")
	public String name;

	public Opportunity() {
	}

}
